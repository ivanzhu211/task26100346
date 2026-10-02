"""执行引擎：调度、重试、崩溃恢复。

不变式：
- 每次提交 SAP 之前，先把尝试记录（result=pending）落库；
- 一个步骤只有在确认上一次尝试确实没有过账后（超过生效期限仍查不到
  已生效凭证）才允许再次提交，因此每个成功步骤在 SAP 里恰好一张凭证；
- 所有状态都在 SQLite 中，进程随时被杀后重启可继续推进。
"""
from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass, field

from .mock_sap import MockSAP, SAPBusinessError, SAPLockConflict, SAPTimeout
from .models import ExecutionStatus, StepStatus, Workflow
from .store import Store


@dataclass
class EngineConfig:
    backoffs: tuple[float, ...] = (5.0, 15.0, 45.0)  # 第 1/2/3 次重试前的退避
    max_retries: int = 3                             # 最多重试 3 次（共提交 4 次）
    effect_deadline: float = 120.0                   # 受理后生效最后期限
    poll_interval: float = 0.5                       # 等待生效时的轮询间隔


class Engine:
    """RPA 执行引擎。同一时刻只允许一个进程运行。"""

    def __init__(
        self,
        db_path: str,
        sap: MockSAP,
        config: EngineConfig | None = None,
    ) -> None:
        self.store = Store(db_path)
        self.sap = sap
        self.config = config or EngineConfig()
        self._recover()

    # -- 崩溃恢复 ----------------------------------------------------------

    def _recover(self) -> None:
        """把被杀时悬空的尝试（已落库但未记录结果）转为等待生效状态。

        这类尝试可能已被 SAP 受理，绝不能直接重提交；按超时处理，
        等生效期限过后确认未过账再重试。
        """
        conn = self.store.conn
        rows = conn.execute(
            "SELECT a.id AS attempt_id, a.execution_id, a.step_idx, a.started_at"
            " FROM attempts a"
            " JOIN steps s ON s.execution_id = a.execution_id AND s.step_idx = a.step_idx"
            " WHERE a.result = 'pending' AND s.status IN ('pending', 'wait_retry')"
        ).fetchall()
        now = time.time()
        for row in rows:
            conn.execute(
                "UPDATE attempts SET result = 'timeout',"
                " error = 'engine killed before result recorded', duration = 0"
                " WHERE id = ?",
                (row["attempt_id"],),
            )
            conn.execute(
                "UPDATE steps SET status = 'wait_effective', effect_deadline = ?,"
                " next_action_at = ? WHERE execution_id = ? AND step_idx = ?",
                (row["started_at"] + self.config.effect_deadline, now,
                 row["execution_id"], row["step_idx"]),
            )
        conn.commit()

    # -- 登记执行 ----------------------------------------------------------

    def submit_execution(self, workflow: Workflow) -> int:
        """登记一个新执行，返回执行 ID。"""
        now = time.time()
        conn = self.store.conn
        wf_json = json.dumps([
            {
                "posting_type": s.posting_type.value,
                "quantity": s.quantity,
                "order_step": s.order_step,
                "receipt_step": s.receipt_step,
            }
            for s in workflow.steps
        ])
        cur = conn.execute(
            "INSERT INTO executions (status, workflow_json, current_step,"
            " created_at, updated_at) VALUES ('running', ?, 0, ?, ?)",
            (wf_json, now, now),
        )
        execution_id = cur.lastrowid
        for idx, step in enumerate(workflow.steps):
            conn.execute(
                "INSERT INTO steps (execution_id, step_idx, posting_type,"
                " reference, quantity, order_step, receipt_step, status,"
                " retries, next_action_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?)",
                (
                    execution_id,
                    idx,
                    step.posting_type.value,
                    self._reference(execution_id, idx),
                    step.quantity,
                    step.order_step,
                    step.receipt_step,
                    "pending" if idx == 0 else "wait_retry",
                    now if idx == 0 else float("inf"),
                ),
            )
        conn.commit()
        return execution_id

    @staticmethod
    def _reference(execution_id: int, step_idx: int) -> str:
        """引擎为每个步骤生成的唯一参考号（≤16 字符），用于事后对账。"""
        return f"E{execution_id}S{step_idx}"[:16]

    # -- 调度 --------------------------------------------------------------

    def run_once(self) -> bool:
        """推进所有到期的执行一步。返回是否还有未完结执行。"""
        now = time.time()
        conn = self.store.conn
        # 按最近推进时间排序并限量，保证进程短寿命时所有执行都能轮转到，
        # 不会因扫描顺序靠后而饿死
        running = conn.execute(
            "SELECT id, current_step FROM executions WHERE status = 'running'"
            " ORDER BY updated_at ASC LIMIT 100"
        ).fetchall()
        for execution in running:
            conn.execute(
                "UPDATE executions SET updated_at = ? WHERE id = ?",
                (now, execution["id"]),
            )
            step = conn.execute(
                "SELECT * FROM steps WHERE execution_id = ? AND step_idx = ?",
                (execution["id"], execution["current_step"]),
            ).fetchone()
            if step is None or step["next_action_at"] > now:
                continue
            if step["status"] in ("pending", "wait_retry"):
                self._submit_step(step, now)
            elif step["status"] == "wait_effective":
                self._check_effective(step, now)
        conn.commit()
        return conn.execute(
            "SELECT 1 FROM executions WHERE status = 'running' LIMIT 1"
        ).fetchone() is not None

    def run_forever(self) -> None:
        """持续调度直到所有执行完结。"""
        while self.run_once():
            conn = self.store.conn
            row = conn.execute(
                "SELECT MIN(s.next_action_at) AS nxt FROM steps s"
                " JOIN executions e ON e.id = s.execution_id"
                " WHERE e.status = 'running' AND s.step_idx = e.current_step"
            ).fetchone()
            nxt = row["nxt"] if row and row["nxt"] is not None else time.time()
            time.sleep(max(0.01, min(nxt - time.time(), 0.5)))

    # -- 提交与检查 ----------------------------------------------------------

    def _resolve_params(self, step: sqlite3.Row) -> dict:
        params = {
            "posting_type": step["posting_type"],
            "reference": step["reference"],
            "quantity": step["quantity"],
        }
        conn = self.store.conn
        for key, col in (("order_no", "order_step"), ("receipt_no", "receipt_step")):
            dep_idx = step[col]
            if dep_idx is not None:
                dep = conn.execute(
                    "SELECT doc_no FROM steps WHERE execution_id = ? AND step_idx = ?",
                    (step["execution_id"], dep_idx),
                ).fetchone()
                params[key] = dep["doc_no"] if dep else None
        return params

    def _submit_step(self, step: sqlite3.Row, now: float) -> None:
        conn = self.store.conn
        params = self._resolve_params(step)
        seq = step["retries"] + 1
        cur = conn.execute(
            "INSERT INTO attempts (execution_id, step_idx, seq, payload_json,"
            " result, started_at) VALUES (?, ?, ?, ?, 'pending', ?)",
            (step["execution_id"], step["step_idx"], seq, json.dumps(params), now),
        )
        attempt_id = cur.lastrowid
        conn.commit()  # 先落库，再调 SAP

        result, doc_no, error = "accepted", None, None
        try:
            doc_no = self.sap.submit(**params)
        except SAPLockConflict as exc:
            result, error = "lock_conflict", str(exc)
        except SAPBusinessError as exc:
            result, error = "business_error", str(exc)
        except SAPTimeout as exc:
            result, error = "timeout", str(exc)
        duration = time.time() - now

        conn.execute(
            "UPDATE attempts SET result = ?, doc_no = ?, error = ?, duration = ?"
            " WHERE id = ?",
            (result, doc_no, error, duration, attempt_id),
        )
        if result == "accepted":
            conn.execute(
                "UPDATE steps SET status = 'wait_effective', effect_deadline = ?,"
                " next_action_at = ? WHERE execution_id = ? AND step_idx = ?",
                (now + self.config.effect_deadline,
                 now + self.config.poll_interval,
                 step["execution_id"], step["step_idx"]),
            )
        elif result == "timeout":
            # 是否受理未知：按等待生效处理，期限过后确认未过账才重试
            conn.execute(
                "UPDATE steps SET status = 'wait_effective', effect_deadline = ?,"
                " next_action_at = ? WHERE execution_id = ? AND step_idx = ?",
                (now + self.config.effect_deadline,
                 now + self.config.poll_interval,
                 step["execution_id"], step["step_idx"]),
            )
        elif result == "lock_conflict":
            self._fail_attempt(step, now)
        else:  # business_error：不重试，直接失败
            self._fail_step(step)
        conn.commit()

    def _check_effective(self, step: sqlite3.Row, now: float) -> None:
        conn = self.store.conn
        doc = self.sap.get_by_reference(step["reference"])
        if doc is not None:
            self._complete_step(step, str(doc["doc_no"]), now)
            return
        if now >= step["effect_deadline"]:
            # 已确认没有过账（未受理或更新中止），可以安全重试
            self._fail_attempt(step, now)
        else:
            conn.execute(
                "UPDATE steps SET next_action_at = ? WHERE execution_id = ?"
                " AND step_idx = ?",
                (now + self.config.poll_interval, step["execution_id"], step["step_idx"]),
            )
        conn.commit()

    # -- 状态推进 ----------------------------------------------------------

    def _complete_step(self, step: sqlite3.Row, doc_no: str, now: float) -> None:
        conn = self.store.conn
        conn.execute(
            "UPDATE steps SET status = 'done', doc_no = ? WHERE execution_id = ?"
            " AND step_idx = ?",
            (doc_no, step["execution_id"], step["step_idx"]),
        )
        execution = conn.execute(
            "SELECT current_step, workflow_json FROM executions WHERE id = ?",
            (step["execution_id"],),
        ).fetchone()
        total = len(json.loads(execution["workflow_json"]))
        next_idx = execution["current_step"] + 1
        if next_idx >= total:
            conn.execute(
                "UPDATE executions SET status = 'completed', current_step = ?,"
                " updated_at = ? WHERE id = ?",
                (next_idx, now, step["execution_id"]),
            )
        else:
            conn.execute(
                "UPDATE executions SET current_step = ?, updated_at = ? WHERE id = ?",
                (next_idx, now, step["execution_id"]),
            )
            conn.execute(
                "UPDATE steps SET status = 'pending', next_action_at = ?"
                " WHERE execution_id = ? AND step_idx = ?",
                (now, step["execution_id"], next_idx),
            )

    def _fail_attempt(self, step: sqlite3.Row, now: float) -> None:
        """一次临时失败：重试次数 +1，还有额度则按阶梯退避后重试。"""
        conn = self.store.conn
        retries = step["retries"] + 1
        if retries > self.config.max_retries:
            self._fail_step(step)
            return
        backoff = self.config.backoffs[min(retries, len(self.config.backoffs)) - 1]
        conn.execute(
            "UPDATE steps SET status = 'wait_retry', retries = ?,"
            " next_action_at = ? WHERE execution_id = ? AND step_idx = ?",
            (retries, now + backoff, step["execution_id"], step["step_idx"]),
        )

    def _fail_step(self, step: sqlite3.Row) -> None:
        conn = self.store.conn
        conn.execute(
            "UPDATE steps SET status = 'failed' WHERE execution_id = ? AND step_idx = ?",
            (step["execution_id"], step["step_idx"]),
        )
        conn.execute(
            "UPDATE executions SET status = 'failed', updated_at = ? WHERE id = ?",
            (time.time(), step["execution_id"]),
        )

    # -- 查询 --------------------------------------------------------------

    def execution_status(self, execution_id: int) -> str:
        row = self.store.conn.execute(
            "SELECT status FROM executions WHERE id = ?", (execution_id,)
        ).fetchone()
        return row["status"]

    def has_running(self) -> bool:
        return self.store.conn.execute(
            "SELECT 1 FROM executions WHERE status = 'running' LIMIT 1"
        ).fetchone() is not None

    def close(self) -> None:
        self.store.close()
