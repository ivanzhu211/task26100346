"""共享数据模型与枚举（骨架）。"""
from __future__ import annotations

import enum
from dataclasses import dataclass, field


class PostingType(enum.Enum):
    CREATE_ORDER = "create_order"
    GOODS_RECEIPT = "goods_receipt"
    INVOICE = "invoice"


class ExecutionStatus(enum.Enum):
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class StepStatus(enum.Enum):
    PENDING = "pending"
    WAIT_RETRY = "wait_retry"
    WAIT_EFFECTIVE = "wait_effective"
    DONE = "done"
    FAILED = "failed"


class AttemptResult(enum.Enum):
    PENDING = "pending"
    ACCEPTED = "accepted"
    LOCK_CONFLICT = "lock_conflict"
    BUSINESS_ERROR = "business_error"
    TIMEOUT = "timeout"


@dataclass(frozen=True)
class Step:
    posting_type: PostingType
    quantity: float = 0.0
    order_step: int | None = None
    receipt_step: int | None = None


@dataclass(frozen=True)
class Workflow:
    steps: tuple[Step, ...]

    def __post_init__(self) -> None:
        if not 1 <= len(self.steps) <= 50:
            raise ValueError("workflow must have 1..50 steps")
