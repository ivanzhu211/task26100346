"""可作为子进程运行的引擎入口（用于崩溃演练）。

用法：
    python -m rpa_engine.runner --engine-db E.db --sap-db S.db \
        [--sap-config '{"timeout_prob": 0.2}'] [--engine-config '{}'] \
        [--seed 42]
执行登记由调用方（上层平台或测试）提前写入引擎库，本进程只负责推进。
"""
from __future__ import annotations

import argparse
import dataclasses
import json

from .engine import Engine, EngineConfig
from .mock_sap import MockSAP, SAPConfig


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--engine-db", required=True)
    parser.add_argument("--sap-db", required=True)
    parser.add_argument("--sap-config", default="{}")
    parser.add_argument("--engine-config", default="{}")
    parser.add_argument("--seed", type=int, default=None)
    args = parser.parse_args()

    sap_cfg = SAPConfig(**{
        k: v for k, v in json.loads(args.sap_config).items()
        if k in {f.name for f in dataclasses.fields(SAPConfig)}
    })
    eng_cfg = EngineConfig(**{
        k: (tuple(v) if k == "backoffs" else v)
        for k, v in json.loads(args.engine_config).items()
        if k in {f.name for f in dataclasses.fields(EngineConfig)}
    })
    sap = MockSAP(args.sap_db, sap_cfg, seed=args.seed)
    engine = Engine(args.engine_db, sap, eng_cfg)
    try:
        engine.run_forever()
    finally:
        engine.close()
        sap.close()


if __name__ == "__main__":
    main()
