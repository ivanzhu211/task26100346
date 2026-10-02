"""引擎的SQLite持久化层：执行、步骤、尝试记录三张表。"""
from __future__ import annotations

import json
import sqlite3
import threading
import time

from .models import DocType, ExecutionStatus, Step, StepStatus, Workflow

_SCHEMA = """
CREATE TABLE IF NOT EXISTS executions (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    workflow     TEXT NOT NULL,          -- JSON序列化的工作流
    status       TEXT NOT NULL,          -- running | completed | failed
    current_step INTEGER NOT NULL DEFAULT 0,
    created_at   REAL NOT NULL,
    updated_at   REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS steps (
    execution_id    INTEGER NOT NULL,
    step_idx        INTEGER NOT NULL,
    reference       TEXT NOT NULL,       -- 确定性参考字段 E{exec}S{idx}
    status          TEXT NOT NULL,       -- pending | running | done | failed
    doc_no          TEXT,                -- 生效的凭证号
    attempts        INTEGER NOT NULL DEFAULT 0,  -- 已提交次数
    next_action_at  REAL NOT NULL DEFAULT 0,     -- 下次可行动时间（退避/轮询）
    uncertain_until REAL,                -- 此前尝试结果未定的确认截止时刻
    error           TEXT,
    PRIMARY KEY (execution_id, step_idx)
);
CREATE TABLE IF NOT EXISTS attempts (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    execution_id INTEGER NOT NULL,
    step_idx     INTEGER NOT NULL,
    posting      TEXT NOT NULL,          -- JSON序列化的提交内容
    result       TEXT,                   -- accepted|lock_conflict|business_error|timeout|interrupted
    doc_no       TEXT,
    duration     REAL,                   -- 耗时（秒）
    created_at   REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_attempts_step ON attempts(execution_id, step_idx);
"""


def make_reference(execution_id: int, step_idx: int) -> str:
    """每步唯一的参考字段，用于在SAP里识别本步的凭证（<=16字符）。"""
    return f"E{execution_id}S{step_idx}"


def workflow_to_json(workflow: Workflow) -> str:
    return json.dumps(
        [
            {
                "doc_type": s.doc_type.value,
                "quantity": s.quantity,
                "order_step": s.order_step,
                "receipt_step": s.receipt_step,
            }
            for s in workflow.steps
        ]
    )


def workflow_from_json(text: str) -> Workflow:
    steps = [
        Step(
            doc_type=DocType(s["doc_type"]),
            quantity=s["quantity"],
            order_step=s["order_step"],
            receipt_step=s["receipt_step"],
        )
        for s in json.loads(text)
    ]
    return Workflow(steps=steps)


class Store:
    """引擎数据库。所有写操作立即提交，保证被杀后状态不丢。"""

    def __init__(self, db_path: str) -> None:
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=FULL")
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.Lock()
        with self._lock, self._conn:
            self._conn.executescript(_SCHEMA)

    def close(self) -> None:
        self._conn.close()

    # ---------- 执行 ----------

    def create_execution(self, workflow: Workflow) -> int:
        now = time.time()
        with self._lock, self._conn:
            cur = self._conn.execute(
                "INSERT INTO executions (workflow, status, current_step, created_at, updated_at)"
                " VALUES (?,?,0,?,?)",
                (workflow_to_json(workflow), ExecutionStatus.RUNNING.value, now, now),
            )
            execution_id = cur.lastrowid
            for idx in range(len(workflow.steps)):
                self._conn.execute(
                    "INSERT INTO steps (execution_id, step_idx, reference, status)"
                    " VALUES (?,?,?,?)",
                    (
                        execution_id,
                        idx,
                        make_reference(execution_id, idx),
                        StepStatus.PENDING.value,
                    ),
                )
        return execution_id

    def get_execution(self, execution_id: int) -> sqlite.Row | None:
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM executions WHERE id = ?", (execution_id,)
            ).fetchone()

    def count_executions(self) -> int:
        with self._lock:
            row = self._conn.execute("SELECT COUNT(*) AS c FROM executions").fetchone()
        return row["c"]

    def count_running_executions(self) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) AS c FROM executions WHERE status = ?",
                (ExecutionStatus.RUNNING.value,),
            ).fetchone()
        return row["c"]

    def advance_execution(self, execution_id: int, next_step: int) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE executions SET current_step = ?, updated_at = ? WHERE id = ?",
                (next_step, time.time(), execution_id),
            )

    def finish_execution(self, execution_id: int, status: ExecutionStatus) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE executions SET status = ?, updated_at = ? WHERE id = ?",
                (status.value, time.time(), execution_id),
            )

    # ---------- 步骤 ----------

    def get_step(self, execution_id: int, step_idx: int) -> sqlite3.Row | None:
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM steps WHERE execution_id = ? AND step_idx = ?",
                (execution_id, step_idx),
            ).fetchone()

    def fetch_due_steps(self, now: float, limit: int) -> list[sqlite3.Row]:
        """取到期的步骤并原子标记为running，防止重复派发。"""
        with self._lock, self._conn:
            rows = self._conn.execute(
                "SELECT s.* FROM steps s"
                " JOIN executions e ON e.id = s.execution_id"
                " WHERE s.status = ? AND s.next_action_at <= ?"
                "   AND e.status = ? AND e.current_step = s.step_idx"
                " ORDER BY s.next_action_at LIMIT ?",
                (StepStatus.PENDING.value, now, ExecutionStatus.RUNNING.value, limit),
            ).fetchall()
            for row in rows:
                self._conn.execute(
                    "UPDATE steps SET status = ?"
                    " WHERE execution_id = ? AND step_idx = ? AND status = ?",
                    (
                        StepStatus.RUNNING.value,
                        row["execution_id"],
                        row["step_idx"],
                        StepStatus.PENDING.value,
                    ),
                )
        return rows

    def update_step(
        self,
        execution_id: int,
        step_idx: int,
        *,
        status: StepStatus | None = None,
        doc_no: str | None = None,
        attempts: int | None = None,
        next_action_at: float | None = None,
        uncertain_until: float | None | object = ...,  # 用None显式清除
        error: str | None = None,
    ) -> None:
        sets, params = [], []
        if status is not None:
            sets.append("status = ?")
            params.append(status.value)
        if doc_no is not None:
            sets.append("doc_no = ?")
            params.append(doc_no)
        if attempts is not None:
            sets.append("attempts = ?")
            params.append(attempts)
        if next_action_at is not None:
            sets.append("next_action_at = ?")
            params.append(next_action_at)
        if uncertain_until is not ...:
            sets.append("uncertain_until = ?")
            params.append(uncertain_until)
        if error is not None:
            sets.append("error = ?")
            params.append(error)
        if not sets:
            return
        params.extend([execution_id, step_idx])
        with self._lock, self._conn:
            self._conn.execute(
                f"UPDATE steps SET {', '.join(set for set in sets)}"
                " WHERE execution_id = ? AND step_idx = ?",
                params,
            )

    def reset_interrupted_steps(self, effective_window: float) -> int:
        """重启恢复：running状态的步骤是被杀时正在处理的，重置为pending。

        被杀时结果未落库的尝试（含已受理/已超时但状态没来得及写回步骤的），
        都按结果未知处理：必须等生效窗口过后确认未过账才能再次提交。
        """
        now = time.time()
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE attempts SET result = 'interrupted', duration = NULL"
                " WHERE result IS NULL"
            )
            rows = self._conn.execute(
                "SELECT s.execution_id, s.step_idx, a.result, a.created_at"
                " FROM steps s JOIN attempts a ON a.id = ("
                "   SELECT id FROM attempts"
                "   WHERE execution_id = s.execution_id AND step_idx = s.step_idx"
                "   ORDER BY id DESC LIMIT 1)"
                " WHERE s.status IN (?, ?) AND a.result IN"
                "   ('accepted', 'timeout', 'interrupted')",
                (StepStatus.PENDING.value, StepStatus.RUNNING.value),
            ).fetchall()
            for row in rows:
                uncertain_until = row["created_at"] + effective_window
                self._conn.execute(
                    "UPDATE steps SET uncertain_until = MAX(COALESCE(uncertain_until, 0), ?)"
                    " WHERE execution_id = ? AND step_idx = ?",
                    (uncertain_until, row["execution_id"], row["step_idx"]),
                )
            cur = self._conn.execute(
                "UPDATE steps SET status = ?, next_action_at = ?"
                " WHERE status = ?",
                (StepStatus.PENDING.value, now, StepStatus.RUNNING.value),
            )
        return cur.rowcount

    def recover_executions(self) -> None:
        """重启恢复：推进/终结那些步骤已定论但执行状态没来得及写回的执行。"""
        with self._lock, self._conn:
            rows = self._conn.execute(
                "SELECT e.id, e.current_step, s.status,"
                " (SELECT COUNT(*) FROM steps WHERE execution_id = e.id) AS n"
                " FROM executions e"
                " JOIN steps s ON s.execution_id = e.id AND s.step_idx = e.current_step"
                " WHERE e.status = ? AND s.status IN (?, ?)",
                (
                    ExecutionStatus.RUNNING.value,
                    StepStatus.DONE.value,
                    StepStatus.FAILED.value,
                ),
            ).fetchall()
            for row in rows:
                if row["status"] == StepStatus.FAILED.value:
                    self._conn.execute(
                        "UPDATE executions SET status = ?, updated_at = ? WHERE id = ?",
                        (ExecutionStatus.FAILED.value, time.time(), row["id"]),
                    )
                elif row["current_step"] + 1 >= row["n"]:
                    self._conn.execute(
                        "UPDATE executions SET status = ?, updated_at = ? WHERE id = ?",
                        (ExecutionStatus.COMPLETED.value, time.time(), row["id"]),
                    )
                else:
                    self._conn.execute(
                        "UPDATE executions SET current_step = ?, updated_at = ?"
                        " WHERE id = ?",
                        (row["current_step"] + 1, time.time(), row["id"]),
                    )

    # ---------- 尝试记录 ----------

    def begin_attempt(self, execution_id: int, step_idx: int, posting: dict) -> int:
        """提交前先落尝试记录（result为空），保证SAP收到的每次提交都有记录。"""
        with self._lock, self._conn:
            cur = self._conn.execute(
                "INSERT INTO attempts (execution_id, step_idx, posting, created_at)"
                " VALUES (?,?,?,?)",
                (execution_id, step_idx, json.dumps(posting), time.time()),
            )
            attempt_id = cur.lastrowid
            self._conn.execute(
                "UPDATE steps SET attempts = attempts + 1"
                " WHERE execution_id = ? AND step_idx = ?",
                (execution_id, step_idx),
            )
        return attempt_id

    def finish_attempt(
        self,
        attempt_id: int,
        result: str,
        duration: float,
        doc_no: str | None = None,
    ) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE attempts SET result = ?, duration = ?, doc_no = ? WHERE id = ?",
                (result, duration, doc_no, attempt_id),
            )

    def list_attempts(self, execution_id: int | None = None) -> list[sqlite3.Row]:
        with self._lock:
            if execution_id is None:
                return self._conn.execute(
                    "SELECT * FROM attempts ORDER BY id"
                ).fetchall()
            return self._conn.execute(
                "SELECT * FROM attempts WHERE execution_id = ? ORDER BY id",
                (execution_id,),
            ).fetchall()

    def count_submitted_attempts(self) -> int:
        """已真正发给SAP的尝试数（不含被杀时可能未发出的，由调用方按result过滤）。"""
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) AS c FROM attempts WHERE result IS NOT NULL"
            ).fetchone()
        return row["c"]
