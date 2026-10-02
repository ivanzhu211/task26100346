"""引擎自身的 SQLite 持久化。

三张表：
- executions：一次工作流执行的整体状态；
- steps：每个步骤的状态、已重试次数、下一次可行动时刻；
- attempts：每一次提交的记录（提交内容、结果、耗时），
  先落库再调用 SAP，保证 SAP 收到的每次提交都能在此找到。
"""
from __future__ import annotations

import sqlite3

_SCHEMA = """
CREATE TABLE IF NOT EXISTS executions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    status TEXT NOT NULL,            -- running / completed / failed
    workflow_json TEXT NOT NULL,
    current_step INTEGER NOT NULL DEFAULT 0,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS steps (
    execution_id INTEGER NOT NULL,
    step_idx INTEGER NOT NULL,
    posting_type TEXT NOT NULL,
    reference TEXT NOT NULL,
    quantity REAL NOT NULL,
    order_step INTEGER,
    receipt_step INTEGER,
    status TEXT NOT NULL,            -- pending/wait_retry/wait_effective/done/failed
    doc_no TEXT,                     -- 已生效的 SAP 凭证号
    retries INTEGER NOT NULL DEFAULT 0,        -- 已失败的尝试次数
    next_action_at REAL NOT NULL,              -- 下一次可提交/可检查的时刻
    effect_deadline REAL,                      -- 已受理提交的最后生效期限
    PRIMARY KEY (execution_id, step_idx)
);

CREATE TABLE IF NOT EXISTS attempts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    execution_id INTEGER NOT NULL,
    step_idx INTEGER NOT NULL,
    seq INTEGER NOT NULL,            -- 该步骤第几次提交（从 1 开始）
    payload_json TEXT NOT NULL,      -- 提交内容
    result TEXT NOT NULL,            -- pending/accepted/lock_conflict/business_error/timeout
    doc_no TEXT,                     -- 受理时 SAP 返回的凭证号
    error TEXT,
    started_at REAL NOT NULL,
    duration REAL NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_attempts_step ON attempts(execution_id, step_idx);
"""


class Store:
    """引擎状态：执行、步骤、尝试记录。"""

    def __init__(self, db_path: str) -> None:
        self._conn = sqlite3.connect(db_path)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        # 进程被杀不丢已提交数据（WAL），仅操作系统崩溃才可能丢末批提交
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    @property
    def conn(self) -> sqlite3.Connection:
        return self._conn

    def close(self) -> None:
        self._conn.close()
