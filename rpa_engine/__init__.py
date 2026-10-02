"""RPA 执行引擎 + 模拟 SAP。

引擎数据与模拟 SAP 数据分别存放在两个独立的 SQLite 文件中。
本包为库形式，由上层自动化平台直接调用，不含接口层。
"""

from .mock_sap import MockSAP, SAPConfig, SAPTimeout, SAPBusinessError, SAPLockConflict
from .engine import Engine, EngineConfig
from .models import (
    Step,
    Workflow,
    ExecutionStatus,
    StepStatus,
    AttemptResult,
    PostingType,
)

__all__ = [
    "MockSAP",
    "SAPConfig",
    "SAPTimeout",
    "SAPBusinessError",
    "SAPLockConflict",
    "Engine",
    "EngineConfig",
    "Step",
    "Workflow",
    "ExecutionStatus",
    "StepStatus",
    "AttemptResult",
    "PostingType",
]
