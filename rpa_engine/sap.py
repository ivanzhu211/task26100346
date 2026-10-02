"""模拟SAP：独立SQLite存储，复现受理/锁冲突/业务错误/超时四种结果。"""
from __future__ import annotations

import random
import sqlite3
import threading
import time
from dataclasses import dataclass

from .models import DocType, Posting, SubmitResult, SubmitResultType

MAX_REFERENCE_LEN = 16

_DOC_PREFIX = {
    DocType.PURCHASE_ORDER: "45",
    DocType.GOODS_RECEIPT: "50",
    DocType.INVOICE: "51",
}

_SCHEMA = """
CREATE TABLE IF NOT EXISTS documents (
    doc_no      TEXT PRIMARY KEY,
    doc_type    TEXT NOT NULL,
    reference   TEXT NOT NULL,
    quantity    REAL NOT NULL,
    order_no    TEXT,
    receipt_no  TEXT,
    status      TEXT NOT NULL,          -- pending | effective | aborted
    created_at  REAL NOT NULL,
    resolve_at  REAL NOT NULL,          -- 异步更新落点（生效或中止）时间
    will_abort  INTEGER NOT NULL,       -- 受理时即决定更新是否中止
    resolved_at REAL
);
CREATE INDEX IF NOT EXISTS idx_documents_reference ON documents(reference);
CREATE INDEX IF NOT EXISTS idx_documents_status ON documents(status);
CREATE TABLE IF NOT EXISTS counters (
    name  TEXT PRIMARY KEY,
    value INTEGER NOT NULL
);
"""


@dataclass
class SapConfig:
    """模拟SAP的行为配置：各类结果概率与生效延迟。"""

    db_path: str = "sap.db"
    lock_conflict_prob: float = 0.0       # 锁冲突概率
    timeout_prob: float = 0.0             # 超时概率（调用方视角）
    timeout_accepted_prob: float = 1.0    # 超时但实际已受理的概率
    abort_prob: float = 0.0               # 已受理更新被中止的概率
    effective_delay_min: float = 0.05     # 生效延迟下限（秒）
    effective_delay_max: float = 0.2      # 生效延迟上限（秒）
    max_effective_window: float = 120.0   # 受理后生效/中止的最长窗口（秒）
    simulated_timeout_delay: float = 5.0  # 模拟超时时调用方需等待的时长（秒）


class MockSAP:
    """模拟SAP系统。数据存在自己的SQLite文件里，与引擎完全独立。"""

    def __init__(self, config: SapConfig | None = None) -> None:
        self.config = config or SapConfig()
        self._conn: sqlite3.Connection | None = None
        self._db_lock = threading.Lock()
        self._stop = threading.Event()
        self._resolver: threading.Thread | None = None

    # ---------- 生命周期 ----------

    def open(self) -> None:
        """打开数据库并恢复未完成的异步更新。"""
        self._conn = sqlite3.connect(self.config.db_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=FULL")
        with self._db_lock, self._conn:
            self._conn.executescript(_SCHEMA)
        self._stop.clear()
        self._resolver = threading.Thread(target=self._resolve_loop, daemon=True)
        self._resolver.start()

    def close(self) -> None:
        self._stop.set()
        if self._resolver is not None:
            self._resolver.join(timeout=5)
            self._resolver = None
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def __enter__(self) -> "MockSAP":
        self.open()
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # ---------- 提交 ----------

    def submit(self, posting: Posting) -> SubmitResult:
        """提交一笔过账，返回四种结果之一。"""
        cfg = self.config
        error = self._validate(posting)
        if error is not None:
            return SubmitResult(SubmitResultType.BUSINESS_ERROR, message=error)
        if random.random() < cfg.lock_conflict_prob:
            return SubmitResult(SubmitResultType.LOCK_CONFLICT, message="凭证被其他会话锁定")
        if random.random() < cfg.timeout_prob:
            return self._submit_timeout(posting)
        doc_no = self._accept(posting)
        return SubmitResult(SubmitResultType.ACCEPTED, doc_no=doc_no)

    def _submit_timeout(self, posting: Posting) -> SubmitResult:
        """模拟超时：可能已受理（响应丢失），也可能根本没受理。"""
        if random.random() < self.config.timeout_accepted_prob:
            self._accept(posting)  # 凭证已入账（pending），但调用方收不到应答
        time.sleep(self.config.simulated_timeout_delay)
        return SubmitResult(SubmitResultType.TIMEOUT, message="等待SAP应答超时")

    def _accept(self, posting: Posting) -> str:
        """受理一笔过账：写入pending凭证，异步更新决定生效或中止。"""
        cfg = self.config
        now = time.time()
        delay = random.uniform(cfg.effective_delay_min, cfg.effective_delay_max)
        resolve_at = min(now + delay, now + cfg.max_effective_window)
        will_abort = 1 if random.random() < cfg.abort_prob else 0
        with self._db_lock, self._conn:
            doc_no = self._next_doc_no(posting.doc_type)
            self._conn.execute(
                "INSERT INTO documents"
                " (doc_no, doc_type, reference, quantity, order_no, receipt_no,"
                "  status, created_at, resolve_at, will_abort)"
                " VALUES (?,?,?,?,?,?, 'pending',?,?,?)",
                (
                    doc_no,
                    posting.doc_type.value,
                    posting.reference,
                    posting.quantity,
                    posting.order_no,
                    posting.receipt_no,
                    now,
                    resolve_at,
                    will_abort,
                ),
            )
        return doc_no

    def _next_doc_no(self, doc_type: DocType) -> str:
        row = self._conn.execute(
            "SELECT value FROM counters WHERE name = ?", (doc_type.value,)
        ).fetchone()
        value = (row[0] if row else 0) + 1
        self._conn.execute(
            "INSERT INTO counters (name, value) VALUES (?, ?)"
            " ON CONFLICT(name) DO UPDATE SET value = excluded.value",
            (doc_type.value, value),
        )
        return f"{_DOC_PREFIX[doc_type]}{value:08d}"

    # ---------- 业务校验 ----------

    def _validate(self, posting: Posting) -> str | None:
        """返回业务错误信息；合法则返回None。"""
        if len(posting.reference) > MAX_REFERENCE_LEN:
            return f"参考字段超长（>{MAX_REFERENCE_LEN}字符）"
        if posting.quantity <= 0:
            return "数量必须为正数"
        with self._db_lock:
            if posting.doc_type == DocType.PURCHASE_ORDER:
                return None
            if posting.doc_type == DocType.GOODS_RECEIPT:
                return self._validate_goods_receipt(posting)
            if posting.doc_type == DocType.INVOICE:
                return self._validate_invoice(posting)
        return f"未知凭证类型: {posting.doc_type}"

    def _validate_goods_receipt(self, posting: Posting) -> str | None:
        if not posting.order_no:
            return "收货必须引用采购订单"
        order = self._effective_doc(posting.order_no)
        if order is None or order["doc_type"] != DocType.PURCHASE_ORDER.value:
            return f"采购订单 {posting.order_no} 未生效"
        received = self._effective_qty(DocType.GOODS_RECEIPT, "order_no", posting.order_no)
        if received + posting.quantity > order["quantity"]:
            return "累计收货超过订货数量"
        return None

    def _validate_invoice(self, posting: Posting) -> str | None:
        if not posting.receipt_no:
            return "开票必须引用收货凭证"
        receipt = self._effective_doc(posting.receipt_no)
        if receipt is None or receipt["doc_type"] != DocType.GOODS_RECEIPT.value:
            return f"收货凭证 {posting.receipt_no} 未生效"
        invoiced = self._effective_qty(DocType.INVOICE, "receipt_no", posting.receipt_no)
        if invoiced + posting.quantity > receipt["quantity"]:
            return "累计开票超过收货数量"
        return None

    def _effective_doc(self, doc_no: str) -> dict | None:
        row = self._conn.execute(
            "SELECT doc_no, doc_type, reference, quantity, order_no, receipt_no"
            " FROM documents WHERE doc_no = ? AND status = 'effective'",
            (doc_no,),
        ).fetchone()
        return self._row_to_doc(row) if row else None

    def _effective_qty(self, doc_type: DocType, ref_col: str, ref_no: str) -> float:
        row = self._conn.execute(
            f"SELECT COALESCE(SUM(quantity), 0) FROM documents"
            f" WHERE doc_type = ? AND {ref_col} = ? AND status = 'effective'",
            (doc_type.value, ref_no),
        ).fetchone()
        return row[0]

    # ---------- 查询 ----------

    def get_by_doc_no(self, doc_no: str) -> dict | None:
        """按凭证号查询，只返回已生效的凭证。"""
        with self._db_lock:
            return self._effective_doc(doc_no)

    def get_by_reference(self, reference: str) -> dict | None:
        """按参考字段查询，只返回已生效的凭证。"""
        with self._db_lock:
            row = self._conn.execute(
                "SELECT doc_no, doc_type, reference, quantity, order_no, receipt_no"
                " FROM documents WHERE reference = ? AND status = 'effective'"
                " ORDER BY created_at LIMIT 1",
                (reference,),
            ).fetchone()
        return self._row_to_doc(row) if row else None

    def all_documents(self) -> list[dict]:
        """测试辅助：返回全部凭证（含pending/aborted）及其状态。"""
        with self._db_lock:
            rows = self._conn.execute(
                "SELECT doc_no, doc_type, reference, quantity, order_no, receipt_no,"
                " status FROM documents ORDER BY created_at, doc_no"
            ).fetchall()
        docs = []
        for row in rows:
            doc = self._row_to_doc(row[:6])
            doc["status"] = row[6]
            docs.append(doc)
        return docs

    @staticmethod
    def _row_to_doc(row: tuple) -> dict:
        return {
            "doc_no": row[0],
            "doc_type": row[1],
            "reference": row[2],
            "quantity": row[3],
            "order_no": row[4],
            "receipt_no": row[5],
        }

    # ---------- 异步生效/中止 ----------

    def _resolve_loop(self) -> None:
        """后台线程：到点的pending凭证按受理时的决定生效或中止。"""
        while not self._stop.is_set():
            now = time.time()
            with self._db_lock, self._conn:
                self._conn.execute(
                    "UPDATE documents SET status ="
                    "   CASE will_abort WHEN 1 THEN 'aborted' ELSE 'effective' END,"
                    " resolved_at = ?"
                    " WHERE status = 'pending' AND resolve_at <= ?",
                    (now, now),
                )
            self._stop.wait(0.02)
