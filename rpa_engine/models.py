"""共享类型定义：过账类型、提交结果、工作流步骤。"""
from __future__ import annotations

import enum
from dataclasses import dataclass, field


class DocType(enum.Enum):
    """模拟SAP支持的三种凭证类型。"""

    PURCHASE_ORDER = "PO"   # 采购订单
    GOODS_RECEIPT = "GR"    # 收货
    INVOICE = "INV"         # 发票


class SubmitResultType(enum.Enum):
    """一次提交的四种结果。"""

    ACCEPTED = "accepted"              # 受理，返回凭证号，异步生效
    LOCK_CONFLICT = "lock_conflict"    # 锁冲突，未过账
    BUSINESS_ERROR = "business_error"  # 业务错误，未过账
    TIMEOUT = "timeout"                # 超时，是否受理未知


@dataclass
class SubmitResult:
    """模拟SAP对一次提交的应答。"""

    result_type: SubmitResultType
    doc_no: str | None = None      # 受理时返回的凭证号
    message: str = ""              # 业务错误/锁冲突的说明


@dataclass
class Posting:
    """一笔过账的内容。"""

    doc_type: DocType
    reference: str                 # 调用方填写的参考字段，最长16字符
    quantity: float = 0.0
    order_no: str | None = None    # 收货时引用的采购订单号
    receipt_no: str | None = None  # 开票时引用的收货凭证号


@dataclass
class Step:
    """工作流中的一个步骤；可引用前面步骤产出的凭证号。"""

    doc_type: DocType
    quantity: float
    order_step: int | None = None    # 引用第几步产出的订单号（收货用）
    receipt_step: int | None = None  # 引用第几步产出的收货凭证号（开票用）


@dataclass
class Workflow:
    """一个工作流：最多50个顺序步骤。"""

    steps: list[Step] = field(default_factory=list)


class ExecutionStatus(enum.Enum):
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class StepStatus(enum.Enum):
    PENDING = "pending"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"
