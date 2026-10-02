"""RPA执行引擎：调度工作流执行，保证恰好一次过账语义。"""
from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

from .models import DocType, ExecutionStatus, Posting, StepStatus, SubmitResultType, Workflow
from .sap import MockSAP
from .store import Store, workflow_from_json

logger = logging.getLogger(__name__)


@dataclass
class EngineConfig:
    """引擎配置：重试退避与轮询节奏。"""

    db_path: str = "engine.db"
    max_steps: int = 50
    max_retries: int = 3                    # 临时失败最多重试3次（共4次提交）
    backoff_schedule: tuple = (5.0, 15.0, 45.0)  # 第1/2/3次重试的退避秒数
    submit_timeout: float = 3.0             # 调用SAP的超时时间（秒）
    poll_interval: float = 0.1              # 调度循环间隔（秒）
    effective_poll_interval: float = 0.2    # 等待凭证生效的轮询间隔（秒）
    effective_window: float = 120.0         # 受理后等待生效的最长窗口（秒）
    worker_count: int = 8                   # 执行步骤的工作线程数


class Engine:
    """执行引擎。数据存在自己的SQLite文件里，可随时被杀并重启恢复。"""

    def __init__(self, sap: MockSAP, config: EngineConfig | None = None) -> None:
        self.sap = sap
        self.config = config or EngineConfig()
        self.store = Store(self.config.db_path)
        self._stop = threading.Event()
        self._scheduler: threading.Thread | None = None
        self._executor: ThreadPoolExecutor | None = None

    # ---------- 生命周期 ----------

    def start(self) -> None:
        """启动引擎：恢复未完成的执行并继续推进。"""
        self.store.recover_executions()
        recovered = self.store.reset_interrupted_steps(self.config.effective_window)
        if recovered:
            logger.info("恢复 %d 个被中断的步骤", recovered)
        self._stop.clear()
        self._executor = ThreadPoolExecutor(
            max_workers=self.config.worker_count, thread_name_prefix="rpa-worker"
        )
        self._scheduler = threading.Thread(
            target=self._schedule_loop, name="rpa-scheduler", daemon=True
        )
        self._scheduler.start()

    def stop(self) -> None:
        self._stop.set()
        if self._scheduler is not None:
            self._scheduler.join(timeout=5)
            self._scheduler = None
        if self._executor is not None:
            self._executor.shutdown(wait=False, cancel_futures=True)
            self._executor = None

    def close(self) -> None:
        self.stop()
        self.store.close()

    def __enter__(self) -> "Engine":
        self.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # ---------- 对外接口 ----------

    def submit_workflow(self, workflow: Workflow) -> int:
        """提交一个工作流，返回执行ID。"""
        if not 1 <= len(workflow.steps) <= self.config.max_steps:
            raise ValueError(f"工作流步骤数须在1..{self.config.max_steps}之间")
        return self.store.create_execution(workflow)

    def get_execution_status(self, execution_id: int) -> ExecutionStatus:
        row = self.store.get_execution(execution_id)
        if row is None:
            raise KeyError(f"执行不存在: {execution_id}")
        return ExecutionStatus(row["status"])

    def wait_all(self, timeout: float | None = None) -> bool:
        """等待所有执行到达终态（完成或失败）。"""
        deadline = None if timeout is None else time.time() + timeout
        while self.store.count_running_executions() > 0:
            if deadline is not None and time.time() > deadline:
                return False
            time.sleep(self.config.poll_interval)
        return True

    # ---------- 调度 ----------

    def _schedule_loop(self) -> None:
        """派发到期步骤；等待中的步骤只改下次行动时间，不占工作线程。"""
        cfg = self.config
        while not self._stop.is_set():
            due = self.store.fetch_due_steps(time.time(), limit=cfg.worker_count * 4)
            for row in due:
                self._executor.submit(
                    self._process_step_guarded, row["execution_id"], row["step_idx"]
                )
            self._stop.wait(cfg.poll_interval)

    def _process_step_guarded(self, execution_id: int, step_idx: int) -> None:
        try:
            self._process_step(execution_id, step_idx)
        except Exception:
            logger.exception("步骤处理异常 execution=%s step=%s", execution_id, step_idx)
            # 稍后重试，避免热循环
            self.store.update_step(
                execution_id,
                step_idx,
                status=StepStatus.PENDING,
                next_action_at=time.time() + 1.0,
            )

    # ---------- 步骤状态机 ----------

    def _process_step(self, execution_id: int, step_idx: int) -> None:
        cfg = self.config
        now = time.time()
        step = self.store.get_step(execution_id, step_idx)
        if step is None or step["status"] != StepStatus.RUNNING.value:
            return
        reference = step["reference"]

        # 1) 先查证：SAP里是否已有本步参考字段的生效凭证（去重/恢复的关键）
        doc = self.sap.get_by_reference(reference)
        if doc is not None:
            self._complete_step(execution_id, step_idx, doc["doc_no"])
            return

        # 2) 有结果未定的尝试：等生效窗口过去再定论
        uncertain_until = step["uncertain_until"]
        if uncertain_until is not None:
            if now < uncertain_until:
                self.store.update_step(
                    execution_id,
                    step_idx,
                    status=StepStatus.PENDING,
                    next_action_at=min(uncertain_until, now + cfg.effective_poll_interval),
                )
                return
            # 窗口已过仍未生效：上次尝试未过账（超时未受理）或更新被中止。
            # 按退避时间安排重试，本次不再行动。
            retry_at = now + self._backoff_for(step["attempts"])
            self.store.update_step(execution_id, step_idx, uncertain_until=None)
            self._retry_or_fail(execution_id, step_idx, step["attempts"], retry_at)
            return

        # 3) 重试次数耗尽则失败（最多 max_retries+1 次提交）
        if step["attempts"] > cfg.max_retries:
            self._fail_step(execution_id, step_idx, "临时失败重试次数耗尽")
            return

        # 4) 构造并提交过账
        posting = self._build_posting(execution_id, step_idx)
        if posting is None:  # 引用数据缺失，属内部错误
            self._fail_step(execution_id, step_idx, "无法构造提交内容")
            return
        self._submit(execution_id, step_idx, posting)

    def _submit(self, execution_id: int, step_idx: int, posting: Posting) -> None:
        cfg = self.config
        now = time.time()
        posting_dict = {
            "doc_type": posting.doc_type.value,
            "reference": posting.reference,
            "quantity": posting.quantity,
            "order_no": posting.order_no,
            "receipt_no": posting.receipt_no,
        }
        # 先落尝试记录再提交：SAP收到的每次提交都能在平台找到记录
        attempt_id = self.store.begin_attempt(execution_id, step_idx, posting_dict)

        result_holder: list = []
        done = threading.Event()

        def call_sap() -> None:
            result_holder.append(self.sap.submit(posting))
            done.set()

        threading.Thread(target=call_sap, daemon=True).start()
        finished = done.wait(cfg.submit_timeout)
        duration = time.time() - now

        if not finished:
            # 超时：不知道SAP是否受理，等生效窗口过后按参考字段查证
            self.store.finish_attempt(
                attempt_id, SubmitResultType.TIMEOUT.value, duration
            )
            self.store.update_step(
                execution_id,
                step_idx,
                status=StepStatus.PENDING,
                next_action_at=now + cfg.effective_poll_interval,
                uncertain_until=now + cfg.effective_window,
            )
            return

        result = result_holder[0]
        self.store.finish_attempt(attempt_id, result.result_type.value, duration, result.doc_no)

        if result.result_type == SubmitResultType.ACCEPTED:
            # 已受理：轮询等生效；窗口内未生效则按更新中止退避
            self.store.update_step(
                execution_id,
                step_idx,
                status=StepStatus.PENDING,
                next_action_at=now + cfg.effective_poll_interval,
                uncertain_until=now + cfg.effective_window,
            )
        elif result.result_type == SubmitResultType.LOCK_CONFLICT:
            attempts = self._attempts(execution_id, step_idx)
            self._retry_or_fail(
                execution_id, step_idx, attempts, now + self._backoff_for(attempts)
            )
        elif result.result_type == SubmitResultType.BUSINESS_ERROR:
            self._fail_step(execution_id, step_idx, f"业务错误: {result.message}")
        else:  # SAP自身报告超时（未在submit_timeout内返回）
            self.store.update_step(
                execution_id,
                step_idx,
                status=StepStatus.PENDING,
                next_action_at=now + cfg.effective_poll_interval,
                uncertain_until=now + cfg.effective_window,
            )

    # ---------- 状态机辅助 ----------

    def _backoff_for(self, attempts: int) -> float:
        """第attempts次失败后、第attempts+1次提交前的退避时长。"""
        idx = min(attempts, len(self.config.backoff_schedule)) - 1
        return self.config.backoff_schedule[idx]

    def _complete_step(self, execution_id: int, step_idx: int, doc_no: str) -> None:
        self.store.update_step(
            execution_id, step_idx, status=StepStatus.DONE, doc_no=doc_no,
            uncertain_until=None,
        )
        execution = self.store.get_execution(execution_id)
        workflow = workflow_from_json(execution["workflow"])
        next_step = step_idx + 1
        if next_step >= len(workflow.steps):
            self.store.finish_execution(execution_id, ExecutionStatus.COMPLETED)
        else:
            self.store.advance_execution(execution_id, next_step)

    def _fail_step(self, execution_id: int, step_idx: int, error: str) -> None:
        self.store.update_step(
            execution_id, step_idx, status=StepStatus.FAILED, error=error
        )
        self.store.finish_execution(execution_id, ExecutionStatus.FAILED)

    def _retry_or_fail(
        self, execution_id: int, step_idx: int, attempts: int, retry_at: float
    ) -> bool:
        """安排退避重试；次数耗尽则失败。返回是否还有重试机会。"""
        if attempts > self.config.max_retries:
            self._fail_step(execution_id, step_idx, "临时失败重试次数耗尽")
            return False
        self.store.update_step(
            execution_id, step_idx, status=StepStatus.PENDING, next_action_at=retry_at
        )
        return True

    def _attempts(self, execution_id: int, step_idx: int) -> int:
        step = self.store.get_step(execution_id, step_idx)
        return step["attempts"]

    def _build_posting(self, execution_id: int, step_idx: int) -> Posting | None:
        execution = self.store.get_execution(execution_id)
        workflow = workflow_from_json(execution["workflow"])
        spec = workflow.steps[step_idx]
        reference = self.store.get_step(execution_id, step_idx)["reference"]
        order_no = receipt_no = None
        if spec.order_step is not None:
            row = self.store.get_step(execution_id, spec.order_step)
            order_no = row["doc_no"] if row else None
            if not order_no:
                return None
        if spec.receipt_step is not None:
            row = self.store.get_step(execution_id, spec.receipt_step)
            receipt_no = row["doc_no"] if row else None
            if not receipt_no:
                return None
        return Posting(
            doc_type=spec.doc_type,
            reference=reference,
            quantity=spec.quantity,
            order_no=order_no,
            receipt_no=receipt_no,
        )
