"""模拟 SAP：三种过账、四种提交结果、异步生效。

数据存放在独立的 SQLite 文件中。异步更新用“到点生效”的方式模拟：
受理时写入一条 pending 凭证并记录生效时刻，任何后续操作都会先把
已到点的 pending 凭证物化为 effective 或 aborted，无需后台线程。
"""
from __future__ import annotations

import random
import sqlite3
import time
from dataclasses import dataclass

MAX_REFERENCE_LEN = 16

_TYPE_ORDER = "create_order"
_TYPE_GR = "goods_receipt"
_TYPE_INVOICE = "invoice"
_TYPES = {_TYPE_ORDER, _TYPE_GR, _TYPE_INVOICE}


class SAPTimeout(Exception):
    """调用方超时，无法得知是否被受理。"""


class SAPLockConflict(Exception):
    """锁冲突，什么都没过账。"""


class SAPBusinessError(Exception):
    """业务错误，什么都没过账。"""


@dataclass
class SAPConfig:
    lock_conflict_prob: float = 0.0      # 锁冲突概率
    timeout_prob: float = 0.0            # 超时概率
    timeout_accepted_prob: float = 0.5   # 超时但其实已受理的概率
    abort_prob: float = 0.0              # 已受理但更新中止的概率
    min_effect_delay: float = 0.0        # 生效延迟下限（秒）
    max_effect_delay: float = 1.0        # 生效延迟上限（秒）
    effect_deadline: float = 120.0       # 受理后生效的最后期限（秒）


_SCHEMA = """
CREATE TABLE IF NOT EXISTS documents (
    doc_no INTEGER PRIMARY KEY AUTOINCREMENT,
    doc_type TEXT NOT NULL,
    reference TEXT NOT NULL,
    quantity REAL NOT NULL,
    order_no INTEGER,
    receipt_no INTEGER,
    status TEXT NOT NULL,          -- pending / effective / aborted
    will_abort INTEGER NOT NULL,   -- 受理时已决定：到点生效还是中止
    created_at REAL NOT NULL,
    effective_at REAL NOT NULL     -- 到点物化的时刻
);
CREATE INDEX IF NOT EXISTS idx_documents_reference ON documents(reference);
"""


class MockSAP:
    """模拟 SAP 系统，数据存于独立 SQLite 文件。"""

    def __init__(
        self,
        db_path: str,
        config: SAPConfig | None = None,
        seed: int | None = None,
    ) -> None:
        self._conn = sqlite3.connect(db_path)
        self._conn.execute("PRAGMA journal_mode=WAL")
        # 进程被杀不丢已提交数据（WAL），仅操作系统崩溃才可能丢末批提交
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.executescript(_SCHEMA)
        self._conn.commit()
        self.config = config or SAPConfig()
        self._rng = random.Random(seed)

    # -- 内部 ------------------------------------------------------------

    def _materialize(self, now: float) -> None:
        """把到点的 pending 凭证物化为 effective / aborted。"""
        cur = self._conn.execute(
            "UPDATE documents SET status = CASE will_abort WHEN 1 THEN 'aborted'"
            " ELSE 'effective' END WHERE status = 'pending' AND effective_at <= ?",
            (now,),
        )
        if cur.rowcount:
            self._conn.commit()

    def _effective_doc(self, doc_no: int) -> sqlite3.Row | None:
        self._conn.row_factory = sqlite3.Row
        cur = self._conn.execute(
            "SELECT * FROM documents WHERE doc_no = ? AND status = 'effective'",
            (doc_no,),
        )
        return cur.fetchone()

    def _posted_quantity(self, doc_type: str, parent_col: str, parent_no: int) -> float:
        cur = self._conn.execute(
            f"SELECT COALESCE(SUM(quantity), 0) FROM documents"
            f" WHERE doc_type = ? AND {parent_col} = ? AND status = 'effective'",
            (doc_type, parent_no),
        )
        return cur.fetchone()[0]

    def _validate(
        self,
        posting_type: str,
        reference: str,
        quantity: float,
        order_no: str | None,
        receipt_no: str | None,
    ) -> None:
        if posting_type not in _TYPES:
            raise SAPBusinessError(f"unknown posting type: {posting_type}")
        if len(reference) > MAX_REFERENCE_LEN:
            raise SAPBusinessError("reference longer than 16 characters")
        if posting_type == _TYPE_ORDER:
            return
        if quantity <= 0:
            raise SAPBusinessError("quantity must be positive")
        if posting_type == _TYPE_GR:
            if order_no is None:
                raise SAPBusinessError("goods receipt requires order_no")
            order = self._effective_doc(int(order_no))
            if order is None or order["doc_type"] != _TYPE_ORDER:
                raise SAPBusinessError(f"order {order_no} not effective")
            posted = self._posted_quantity(_TYPE_GR, "order_no", int(order_no))
            if posted + quantity > order["quantity"]:
                raise SAPBusinessError("goods receipt exceeds order quantity")
        else:  # invoice
            if receipt_no is None:
                raise SAPBusinessError("invoice requires receipt_no")
            receipt = self._effective_doc(int(receipt_no))
            if receipt is None or receipt["doc_type"] != _TYPE_GR:
                raise SAPBusinessError(f"receipt {receipt_no} not effective")
            posted = self._posted_quantity(_TYPE_INVOICE, "receipt_no", int(receipt_no))
            if posted + quantity > receipt["quantity"]:
                raise SAPBusinessError("invoice exceeds receipt quantity")

    def _accept(
        self,
        posting_type: str,
        reference: str,
        quantity: float,
        order_no: str | None,
        receipt_no: str | None,
        now: float,
    ) -> str:
        cfg = self.config
        will_abort = 1 if self._rng.random() < cfg.abort_prob else 0
        delay = self._rng.uniform(cfg.min_effect_delay, cfg.max_effect_delay)
        delay = min(delay, cfg.effect_deadline)
        cur = self._conn.execute(
            "INSERT INTO documents (doc_type, reference, quantity, order_no,"
            " receipt_no, status, will_abort, created_at, effective_at)"
            " VALUES (?, ?, ?, ?, ?, 'pending', ?, ?, ?)",
            (
                posting_type,
                reference,
                quantity,
                int(order_no) if order_no is not None else None,
                int(receipt_no) if receipt_no is not None else None,
                will_abort,
                now,
                now + delay,
            ),
        )
        self._conn.commit()
        return str(cur.lastrowid)

    # -- 对外 ------------------------------------------------------------

    def submit(
        self,
        posting_type: str,
        reference: str,
        quantity: float = 0.0,
        order_no: str | None = None,
        receipt_no: str | None = None,
    ) -> str:
        """提交一笔过账。受理则返回凭证号；否则抛对应异常。"""
        now = time.time()
        self._materialize(now)
        self._validate(posting_type, reference, quantity, order_no, receipt_no)
        cfg = self.config
        if self._rng.random() < cfg.lock_conflict_prob:
            raise SAPLockConflict("document locked by another user")
        if self._rng.random() < cfg.timeout_prob:
            if self._rng.random() < cfg.timeout_accepted_prob:
                self._accept(posting_type, reference, quantity, order_no, receipt_no, now)
            raise SAPTimeout("no response from SAP")
        return self._accept(posting_type, reference, quantity, order_no, receipt_no, now)

    def get_by_doc_no(self, doc_no: str) -> dict | None:
        """按凭证号查询，仅返回已生效凭证。"""
        self._materialize(time.time())
        row = self._effective_doc(int(doc_no))
        return dict(row) if row else None

    def get_by_reference(self, reference: str) -> dict | None:
        """按参考字段查询，仅返回已生效凭证。"""
        self._materialize(time.time())
        self._conn.row_factory = sqlite3.Row
        cur = self._conn.execute(
            "SELECT * FROM documents WHERE reference = ? AND status = 'effective'"
            " ORDER BY doc_no",
            (reference,),
        )
        rows = cur.fetchall()
        return dict(rows[0]) if rows else None

    def close(self) -> None:
        self._conn.close()
