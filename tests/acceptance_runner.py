"""验收测试的子进程运行器：创建执行并推进到全部终态。

被测试进程以 SIGKILL 随时杀掉后，用相同参数重启即可继续。
"""
import argparse
import logging
import sys

from rpa_engine import (
    DocType,
    Engine,
    EngineConfig,
    MockSAP,
    SapConfig,
    Step,
    Workflow,
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sap-db", required=True)
    parser.add_argument("--engine-db", required=True)
    parser.add_argument("--executions", type=int, default=200)
    args = parser.parse_args()

    logging.basicConfig(level=logging.WARNING)

    sap = MockSAP(
        SapConfig(
            db_path=args.sap_db,
            lock_conflict_prob=0.15,
            timeout_prob=0.15,
            timeout_accepted_prob=0.8,
            abort_prob=0.15,
            effective_delay_min=0.2,
            effective_delay_max=2.0,   # 拉长的生效延迟
            max_effective_window=8.0,
            simulated_timeout_delay=2.0,
        )
    )
    sap.open()
    engine = Engine(
        sap,
        EngineConfig(
            db_path=args.engine_db,
            submit_timeout=0.8,
            effective_window=8.0,
            poll_interval=0.05,
            effective_poll_interval=0.2,
            backoff_schedule=(0.5, 1.0, 2.0),  # 测试用缩短的退避，比例同5/15/45
            worker_count=16,
        ),
    )
    engine.start()

    missing = args.executions - engine.store.count_executions()
    for _ in range(missing):
        engine.submit_workflow(
            Workflow(
                [
                    Step(DocType.PURCHASE_ORDER, quantity=10),
                    Step(DocType.GOODS_RECEIPT, quantity=10, order_step=0),
                    Step(DocType.INVOICE, quantity=10, receipt_step=1),
                ]
            )
        )

    engine.wait_all()
    running = engine.store.count_running_executions()
    print(f"ALL_TERMINAL running={running}", flush=True)
    engine.close()
    sap.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
