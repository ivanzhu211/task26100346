"""引擎行为测试：业务错误不重试、超时恰好一张、崩溃恢复。"""
import json
import os
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from rpa_engine import (
    Engine, EngineConfig, MockSAP, SAPConfig, Workflow, Step, PostingType,
)

FAST = EngineConfig(backoffs=(0.1, 0.2, 0.3), effect_deadline=0.5, poll_interval=0.02)


def make(tmp, sap_cfg=None, eng_cfg=None):
    sap = MockSAP(os.path.join(tmp, "sap.db"), sap_cfg)
    eng = Engine(os.path.join(tmp, "engine.db"), sap, eng_cfg or FAST)
    return sap, eng


class EngineTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()

    def test_business_error_no_retry(self):
        sap, eng = make(self.dir)
        # 收货步骤引用不存在的前序步骤产出 -> 业务错误
        eid = eng.submit_execution(Workflow(steps=(
            Step(PostingType.GOODS_RECEIPT, quantity=1, order_step=0),
        ),))
        eng.run_forever()
        self.assertEqual(eng.execution_status(eid), "failed")
        n = eng.store.conn.execute(
            "SELECT COUNT(*) FROM attempts WHERE execution_id = ?", (eid,)).fetchone()[0]
        self.assertEqual(n, 1)  # 业务错误不重试
        eng.close(); sap.close()

    def test_timeout_accepted_exactly_one_doc(self):
        # 每次提交都超时但其实已受理：引擎必须等到生效后认账，不得重发
        sap, eng = make(self.dir, SAPConfig(
            timeout_prob=1.0, timeout_accepted_prob=1.0, max_effect_delay=0.2))
        eid = eng.submit_execution(Workflow(steps=(
            Step(PostingType.CREATE_ORDER, quantity=3),
        ),))
        eng.run_forever()
        self.assertEqual(eng.execution_status(eid), "completed")
        docs = sap._conn.execute(
            "SELECT COUNT(*) FROM documents WHERE reference = ? AND status = 'effective'",
            (f"E{eid}S0",)).fetchone()[0]
        self.assertEqual(docs, 1)
        attempts = eng.store.conn.execute(
            "SELECT COUNT(*) FROM attempts WHERE execution_id = ?", (eid,)).fetchone()[0]
        self.assertEqual(attempts, 1)  # 只提交过一次
        eng.close(); sap.close()

    def test_aborted_update_retried_then_one_doc(self):
        # 前两次受理后中止，第三次生效
        sap_cfg = SAPConfig(abort_prob=0.5, max_effect_delay=0.1)
        sap, eng = make(self.dir, sap_cfg)
        # 用脚本化的随机序列：每次 submit 消耗 3 个 random()
        # （锁冲突、超时、中止判定），前两次中止，第三次生效
        draws = iter([
            0.9, 0.9, 0.4,   # 第 1 次：受理后中止
            0.9, 0.9, 0.4,   # 第 2 次：受理后中止
            0.9, 0.9, 0.6,   # 第 3 次：生效
        ])

        class ScriptedRng:
            def random(self):
                return next(draws)

            def uniform(self, a, b):
                return a

        sap._rng = ScriptedRng()
        eid = eng.submit_execution(Workflow(steps=(
            Step(PostingType.CREATE_ORDER, quantity=3),
        ),))
        eng.run_forever()
        self.assertEqual(eng.execution_status(eid), "completed")
        total = sap._conn.execute(
            "SELECT COUNT(*) FROM documents WHERE reference = ?",
            (f"E{eid}S0",)).fetchone()[0]
        self.assertEqual(total, 3)  # 三次受理（两次中止、一次生效）
        docs = sap._conn.execute(
            "SELECT COUNT(*) FROM documents WHERE reference = ? AND status = 'effective'",
            (f"E{eid}S0",)).fetchone()[0]
        self.assertEqual(docs, 1)
        eng.close(); sap.close()

    def test_recover_pending_attempt_after_kill(self):
        # 模拟被杀：尝试记录停留在 pending，重启后不得盲目重发
        sap, eng = make(self.dir)
        eid = eng.submit_execution(Workflow(steps=(
            Step(PostingType.CREATE_ORDER, quantity=3),
        ),))
        # 手工伪造一个 pending 尝试（等价于提交途中被杀）
        eng.store.conn.execute(
            "INSERT INTO attempts (execution_id, step_idx, seq, payload_json,"
            " result, started_at) VALUES (?, 0, 1, ?, 'pending', ?)",
            (eid, json.dumps({"posting_type": "create_order"}), time.time()))
        eng.store.conn.commit()
        eng.close()
        # 重启：恢复逻辑应把该尝试标记为 timeout 并等待生效期限
        eng2 = Engine(os.path.join(self.dir, "engine.db"), sap, FAST)
        row = eng2.store.conn.execute(
            "SELECT result FROM attempts WHERE execution_id = ?", (eid,)).fetchone()
        self.assertEqual(row["result"], "timeout")
        step = eng2.store.conn.execute(
            "SELECT status FROM steps WHERE execution_id = ? AND step_idx = 0",
            (eid,)).fetchone()
        self.assertEqual(step["status"], "wait_effective")
        eng2.run_forever()
        self.assertEqual(eng2.execution_status(eid), "completed")
        docs = sap._conn.execute(
            "SELECT COUNT(*) FROM documents WHERE reference = ? AND status = 'effective'",
            (f"E{eid}S0",)).fetchone()[0]
        self.assertEqual(docs, 1)
        eng2.close(); sap.close()

    def test_attempt_record_written_before_submit(self):
        # SAP 收到的每次提交都必须有对应尝试记录
        sap, eng = make(self.dir, SAPConfig(lock_conflict_prob=0.5), )
        eid = eng.submit_execution(Workflow(steps=(
            Step(PostingType.CREATE_ORDER, quantity=1),
        ),))
        eng.run_forever()
        attempts = eng.store.conn.execute(
            "SELECT payload_json, result, duration FROM attempts"
            " WHERE execution_id = ?", (eid,)).fetchall()
        self.assertGreaterEqual(len(attempts), 1)
        for row in attempts:
            payload = json.loads(row["payload_json"])
            self.assertEqual(payload["reference"], f"E{eid}S0")
            self.assertIn(row["result"],
                          ("accepted", "lock_conflict", "business_error", "timeout"))
            self.assertGreaterEqual(row["duration"], 0)
        eng.close(); sap.close()


if __name__ == "__main__":
    unittest.main()
