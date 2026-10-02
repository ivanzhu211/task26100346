"""模拟 SAP 业务规则与四种提交结果的单元测试。"""
import os
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from rpa_engine import MockSAP, SAPConfig, SAPBusinessError, SAPLockConflict, SAPTimeout


class MockSAPTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.sap = MockSAP(os.path.join(self.dir, "sap.db"))

    def tearDown(self):
        self.sap.close()

    def _wait_effective(self, doc_no, timeout=5.0):
        deadline = time.time() + timeout
        while time.time() < deadline:
            doc = self.sap.get_by_doc_no(doc_no)
            if doc is not None:
                return doc
            time.sleep(0.05)
        self.fail(f"doc {doc_no} not effective in time")

    def test_full_chain_and_query(self):
        order = self.sap.submit("create_order", "REF-A", quantity=10)
        self.assertIsNone(self.sap.get_by_doc_no(order))  # 未生效查不到
        self._wait_effective(order)
        gr = self.sap.submit("goods_receipt", "REF-B", quantity=6, order_no=order)
        self._wait_effective(gr)
        inv = self.sap.submit("invoice", "REF-C", quantity=6, receipt_no=gr)
        doc = self._wait_effective(inv)
        self.assertEqual(doc["reference"], "REF-C")
        self.assertEqual(self.sap.get_by_reference("REF-B")["doc_type"], "goods_receipt")

    def test_reference_too_long(self):
        with self.assertRaises(SAPBusinessError):
            self.sap.submit("create_order", "X" * 17)
        self.sap.submit("create_order", "X" * 16)  # 16 字符合法

    def test_reference_must_be_effective(self):
        order = self.sap.submit("create_order", "REF-O", quantity=5)
        with self.assertRaises(SAPBusinessError):  # 订单还未生效
            self.sap.submit("goods_receipt", "REF-G", quantity=1, order_no=order)
        with self.assertRaises(SAPBusinessError):  # 不存在的订单
            self.sap.submit("goods_receipt", "REF-G", quantity=1, order_no="99999")

    def test_cumulative_quantity_limits(self):
        order = self.sap.submit("create_order", "REF-O2", quantity=5)
        self._wait_effective(order)
        gr1 = self.sap.submit("goods_receipt", "R1", quantity=3, order_no=order)
        self._wait_effective(gr1)
        gr2 = self.sap.submit("goods_receipt", "R2", quantity=2, order_no=order)
        self._wait_effective(gr2)
        with self.assertRaises(SAPBusinessError):  # 累计收货超订货量
            self.sap.submit("goods_receipt", "R3", quantity=1, order_no=order)
        inv1 = self.sap.submit("invoice", "I1", quantity=2, receipt_no=gr1)
        self._wait_effective(inv1)
        with self.assertRaises(SAPBusinessError):  # 累计开票超收货量
            self.sap.submit("invoice", "I2", quantity=2, receipt_no=gr1)

    def test_lock_conflict_posts_nothing(self):
        self.sap.close()
        self.sap = MockSAP(os.path.join(self.dir, "sap.db"),
                           SAPConfig(lock_conflict_prob=1.0))
        with self.assertRaises(SAPLockConflict):
            self.sap.submit("create_order", "REF-L")
        self.assertIsNone(self.sap.get_by_reference("REF-L"))

    def test_timeout_may_or_may_not_post(self):
        self.sap.close()
        # 超时且必受理：凭证之后应生效
        self.sap = MockSAP(os.path.join(self.dir, "sap.db"),
                           SAPConfig(timeout_prob=1.0, timeout_accepted_prob=1.0))
        with self.assertRaises(SAPTimeout):
            self.sap.submit("create_order", "REF-T1")
        deadline = time.time() + 5
        while time.time() < deadline:
            if self.sap.get_by_reference("REF-T1"):
                break
            time.sleep(0.05)
        self.assertIsNotNone(self.sap.get_by_reference("REF-T1"))
        self.sap.close()
        # 超时且必不受理：永远查不到
        self.sap = MockSAP(os.path.join(self.dir, "sap.db"),
                           SAPConfig(timeout_prob=1.0, timeout_accepted_prob=0.0))
        with self.assertRaises(SAPTimeout):
            self.sap.submit("create_order", "REF-T2")
        time.sleep(1.2)
        self.assertIsNone(self.sap.get_by_reference("REF-T2"))

    def test_aborted_update_never_effective(self):
        self.sap.close()
        self.sap = MockSAP(os.path.join(self.dir, "sap.db"),
                           SAPConfig(abort_prob=1.0, max_effect_delay=0.2))
        doc_no = self.sap.submit("create_order", "REF-X")
        time.sleep(0.5)
        self.assertIsNone(self.sap.get_by_doc_no(doc_no))
        self.assertIsNone(self.sap.get_by_reference("REF-X"))


if __name__ == "__main__":
    unittest.main()
