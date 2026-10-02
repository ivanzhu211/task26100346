"""模拟SAP的业务规则与四种提交结果。"""
import os
import tempfile
import time
import unittest

from rpa_engine import DocType, MockSAP, Posting, SapConfig, SubmitResultType


class SapTestBase(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.sap = MockSAP(
            SapConfig(
                db_path=os.path.join(self.dir, "sap.db"),
                effective_delay_min=0.02,
                effective_delay_max=0.05,
            )
        )
        self.sap.open()

    def tearDown(self):
        self.sap.close()

    def submit_and_wait(self, posting, wait=0.3):
        result = self.sap.submit(posting)
        self.assertEqual(result.result_type, SubmitResultType.ACCEPTED)
        time.sleep(wait)
        return result.doc_no

    def make_order(self, reference="PO1", quantity=10):
        return self.submit_and_wait(Posting(DocType.PURCHASE_ORDER, reference, quantity))

    def make_receipt(self, order_no, reference="GR1", quantity=10):
        return self.submit_and_wait(
            Posting(DocType.GOODS_RECEIPT, reference, quantity, order_no=order_no)
        )


class TestBusinessRules(SapTestBase):
    def test_reference_too_long(self):
        result = self.sap.submit(Posting(DocType.PURCHASE_ORDER, "X" * 17, 1))
        self.assertEqual(result.result_type, SubmitResultType.BUSINESS_ERROR)
        result = self.sap.submit(Posting(DocType.PURCHASE_ORDER, "X" * 16, 1))
        self.assertEqual(result.result_type, SubmitResultType.ACCEPTED)

    def test_receipt_requires_effective_order(self):
        result = self.sap.submit(
            Posting(DocType.GOODS_RECEIPT, "GR1", 1, order_no="4599999999")
        )
        self.assertEqual(result.result_type, SubmitResultType.BUSINESS_ERROR)

    def test_receipt_not_before_order_effective(self):
        order = self.sap.submit(Posting(DocType.PURCHASE_ORDER, "PO1", 10))
        # 订单已受理但未生效，引用它收货应报业务错误
        result = self.sap.submit(
            Posting(DocType.GOODS_RECEIPT, "GR1", 1, order_no=order.doc_no)
        )
        self.assertEqual(result.result_type, SubmitResultType.BUSINESS_ERROR)

    def test_cumulative_receipt_cannot_exceed_order(self):
        order_no = self.make_order(quantity=10)
        self.make_receipt(order_no, "GR1", 6)
        self.make_receipt(order_no, "GR2", 4)
        result = self.sap.submit(
            Posting(DocType.GOODS_RECEIPT, "GR3", 1, order_no=order_no)
        )
        self.assertEqual(result.result_type, SubmitResultType.BUSINESS_ERROR)

    def test_cumulative_invoice_cannot_exceed_receipt(self):
        order_no = self.make_order(quantity=10)
        receipt_no = self.make_receipt(order_no, quantity=10)
        self.submit_and_wait(Posting(DocType.INVOICE, "INV1", 7, receipt_no=receipt_no))
        self.submit_and_wait(Posting(DocType.INVOICE, "INV2", 3, receipt_no=receipt_no))
        result = self.sap.submit(Posting(DocType.INVOICE, "INV3", 1, receipt_no=receipt_no))
        self.assertEqual(result.result_type, SubmitResultType.BUSINESS_ERROR)

    def test_query_only_returns_effective(self):
        result = self.sap.submit(Posting(DocType.PURCHASE_ORDER, "PO9", 5))
        self.assertIsNone(self.sap.get_by_doc_no(result.doc_no))
        self.assertIsNone(self.sap.get_by_reference("PO9"))
        time.sleep(0.3)
        self.assertIsNotNone(self.sap.get_by_doc_no(result.doc_no))
        self.assertIsNotNone(self.sap.get_by_reference("PO9"))


class TestResultTypes(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()

    def open_sap(self, **kwargs):
        kwargs.setdefault("db_path", os.path.join(self.dir, "sap.db"))
        kwargs.setdefault("effective_delay_min", 0.02)
        kwargs.setdefault("effective_delay_max", 0.05)
        sap = MockSAP(SapConfig(**kwargs))
        sap.open()
        return sap

    def test_lock_conflict_posts_nothing(self):
        sap = self.open_sap(lock_conflict_prob=1.0)
        try:
            result = sap.submit(Posting(DocType.PURCHASE_ORDER, "PO1", 1))
            self.assertEqual(result.result_type, SubmitResultType.LOCK_CONFLICT)
            self.assertEqual(sap.all_documents(), [])
        finally:
            sap.close()

    def test_timeout_accepted_creates_pending_doc(self):
        sap = self.open_sap(
            timeout_prob=1.0, timeout_accepted_prob=1.0, simulated_timeout_delay=0.05
        )
        try:
            result = sap.submit(Posting(DocType.PURCHASE_ORDER, "PO1", 1))
            self.assertEqual(result.result_type, SubmitResultType.TIMEOUT)
            self.assertIsNone(result.doc_no)
            docs = sap.all_documents()
            self.assertEqual(len(docs), 1)  # 实际已受理
            time.sleep(0.3)
            self.assertIsNotNone(sap.get_by_reference("PO1"))
        finally:
            sap.close()

    def test_timeout_not_accepted_posts_nothing(self):
        sap = self.open_sap(
            timeout_prob=1.0, timeout_accepted_prob=0.0, simulated_timeout_delay=0.05
        )
        try:
            result = sap.submit(Posting(DocType.PURCHASE_ORDER, "PO1", 1))
            self.assertEqual(result.result_type, SubmitResultType.TIMEOUT)
            self.assertEqual(sap.all_documents(), [])
        finally:
            sap.close()

    def test_aborted_update_never_becomes_effective(self):
        sap = self.open_sap(abort_prob=1.0)
        try:
            result = sap.submit(Posting(DocType.PURCHASE_ORDER, "PO1", 1))
            self.assertEqual(result.result_type, SubmitResultType.ACCEPTED)
            time.sleep(0.3)
            self.assertIsNone(sap.get_by_doc_no(result.doc_no))
            self.assertEqual(sap.all_documents()[0]["status"], "aborted")
        finally:
            sap.close()

    def test_pending_updates_resume_after_reopen(self):
        db = os.path.join(self.dir, "sap.db")
        sap = self.open_sap(
            db_path=db, effective_delay_min=60.0, effective_delay_max=60.0
        )
        result = sap.submit(Posting(DocType.PURCHASE_ORDER, "PO1", 1))
        self.assertEqual(result.result_type, SubmitResultType.ACCEPTED)
        sap.close()
        # 重开：pending凭证仍会在到点后生效
        sap2 = self.open_sap(
            db_path=db, effective_delay_min=60.0, effective_delay_max=60.0
        )
        try:
            with sap2._db_lock, sap2._conn:
                sap2._conn.execute("UPDATE documents SET resolve_at = 0")
            time.sleep(0.3)
            self.assertIsNotNone(sap2.get_by_doc_no(result.doc_no))
        finally:
            sap2.close()


if __name__ == "__main__":
    unittest.main()
