# RPA 执行引擎 + 模拟 SAP

采购/财务自动化的底层库：一个在外部 SAP 行为下保证"不重不漏"的执行引擎，
以及一个如实复现 SAP 行为的模拟 SAP。纯 Python 标准库，无外部服务依赖；
引擎数据与模拟 SAP 数据分别存放在两个独立的 SQLite 文件中。

## 组成

- `rpa_engine/mock_sap.py` — 模拟 SAP。支持建采购订单、收货、开票三种过账；
  一次提交有受理 / 锁冲突 / 业务错误 / 超时四种结果；受理的提交异步生效
  （到点物化，无需后台线程），也可能更新中止。各类概率与生效延迟可配置。
- `rpa_engine/engine.py` — 执行引擎。调度、阶梯退避重试（5s/15s/45s，最多
  重试 3 次）、崩溃恢复。单进程运行，随时可被杀，重启后接着推进。
- `rpa_engine/store.py` — 引擎自身的 SQLite 持久化（执行、步骤、尝试记录）。
- `rpa_engine/runner.py` — 可作为子进程运行的入口（用于崩溃演练）。

## 恰好一次的原理

1. 每次提交 SAP 之前，先把尝试记录（含提交内容）写入引擎库；
2. 引擎为每个步骤生成唯一参考号（`E<执行号>S<步骤号>`，≤16 字符）填入 SAP
   参考字段；
3. 超时或结果未知的尝试绝不直接重发：等到生效期限（默认 120 秒）过后，
   按参考字段在 SAP 里查不到已生效凭证，确认确实没有过账才重试；
4. 进程被杀后重启，悬空的尝试记录按"结果未知"走第 3 条路径恢复。

## 用法

```python
from rpa_engine import MockSAP, SAPConfig, Engine, EngineConfig
from rpa_engine import Workflow, Step, PostingType

sap = MockSAP("sap.db", SAPConfig(timeout_prob=0.1, abort_prob=0.1))
engine = Engine("engine.db", sap)

execution_id = engine.submit_execution(Workflow(steps=(
    Step(PostingType.CREATE_ORDER, quantity=10),
    Step(PostingType.GOODS_RECEIPT, quantity=10, order_step=0),
    Step(PostingType.INVOICE, quantity=10, receipt_step=1),
)))
engine.run_forever()   # 或在上层平台的事件循环里反复调用 run_once()
```

## 测试

```bash
python3 -m unittest discover -s tests -v
```

- `tests/test_mock_sap.py` — 模拟 SAP 业务规则与四种提交结果；
- `tests/test_engine.py` — 业务错误不重试、超时恰好一张、崩溃恢复；
- `tests/test_backoff.py` — 验收二：一直锁冲突的步骤恰好提交 4 次，
  间隔依次 ≥ 5s/15s/45s（真实时间，约 65 秒）；
- `tests/test_chaos.py` — 验收一：高故障率下 200 个"建单、收货、开票"
  执行，期间 SIGKILL 引擎进程并重启至少 30 次，结束后逐条核对 SAP 数据。
