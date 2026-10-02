# RPA 执行引擎 + 模拟SAP

采购/财务机器人场景的执行引擎库：在 SAP 这类无法一起提交回滚的外部系统上，
保证**每个成功步骤恰好一张生效凭证、失败步骤一张都没有**。
引擎可随时被直接杀掉，重启后接着推进，重试次数与退避时间点跨重启延续。

## 组成

- `rpa_engine/sap.py` — 模拟SAP。如实复现四种提交结果（受理/锁冲突/业务错误/超时）、
  异步生效或中止（受理后120秒窗口内落定）、业务校验（引用必须已生效、
  累计收货≤订货、累计开票≤收货、参考字段≤16字符）。数据独立存一个SQLite文件。
- `rpa_engine/engine.py` — 执行引擎。调度工作流（最多50个顺序步骤，可引用前序步骤的凭证号），
  临时失败按 5s/15s/45s 退避重试最多3次，业务错误不重试。
  等待退避/生效只改下次行动时间，不阻塞其他执行。数据独立存另一个SQLite文件。
- `rpa_engine/store.py` — 引擎的持久化层（执行、步骤、尝试记录三张表）。

## 恰好一次的原理

- 每步有确定性参考字段 `E{执行ID}S{步骤号}`（≤16字符），随过账写入SAP。
- 提交前先落尝试记录再调用SAP：SAP收到的每次提交都能在平台找到记录。
- 任何一次提交后，先按参考字段查SAP是否已生效，已生效直接认领，绝不重复提交。
- 超时/被杀等结果未知的尝试，必须等生效窗口（默认120秒）过去、
  确认SAP里没有对应凭证，才允许再次提交。
- 所有状态（重试次数、退避时间、未确认尝试的截止时刻）持久化在引擎SQLite里，被杀不丢。

## 用法

```python
from rpa_engine import (DocType, Engine, EngineConfig, MockSAP, SapConfig,
                        Step, Workflow)

sap = MockSAP(SapConfig(db_path="sap.db")); sap.open()
engine = Engine(sap, EngineConfig(db_path="engine.db")); engine.start()

eid = engine.submit_workflow(Workflow([
    Step(DocType.PURCHASE_ORDER, quantity=10),
    Step(DocType.GOODS_RECEIPT, quantity=10, order_step=0),
    Step(DocType.INVOICE, quantity=10, receipt_step=1),
]))
engine.wait_all()          # 等所有执行到达终态
engine.get_execution_status(eid)
```

注意：`EngineConfig.effective_window` 应不小于 `SapConfig.max_effective_window`。

## 测试

```bash
python3 -m unittest discover tests -v
```

- `tests/test_sap.py` — 模拟SAP的业务规则与四种结果。
- `tests/test_engine.py` — 退避节奏（锁冲突步骤恰好4次提交、间隔≥5/15/45秒）、
  重试上限、超时已受理不重复过账、业务错误不重试。
- `tests/test_acceptance.py` — 验收：高故障率+拉长生效延迟下跑200个
  "建单、收货、开票"执行，期间SIGKILL重启30+次，结束后用SAP数据逐个核对
  恰好一次、引用链同属一个执行、每次提交都有尝试记录。
