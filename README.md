# 贸易发票融资台账

面向跨境保理/发票融资的事件溯源台账。出口商把同一批发票分别提交给保理商与银行、
承运人回执迟到、断网补传等情形，都在不可变流水上被识别和量化。

## 核心规则

- **只增流水、可重算**：发票、装运凭证、承运人回执、受让通知、放款、回款、退货、
  拆分、冻结/解冻、汇率全部是追加事件；台账视图是事件流的纯函数折叠，删掉派生
  状态从头重放结果一致（`service/ledger.py` 的 `fold`）。
- **原始凭证不可改**：发票 `summary` 等原始内容只原样留痕，没有任何更新接口；
  重复登记同票号返回 `INVOICE_EXISTS`。
- **幂等**：`event_id`（及 `idempotency_key`）全局唯一。断网补传或服务恢复后
  重放同一标识，原样返回首次回执（含当初被拒绝的结果），不产生第二条流水。
- **重复质押识别**：同一发票族（母票/拆分子票）出现两个不同受让人时，第二份
  受让通知以 `DUPLICATE_ASSIGNMENT` 拒绝，整族系统冻结，依据中保留前手通知
  事件 ID；冻结期间连融资经理都不能放款。
- **迟到承运人回执**：在放款/回款/退货之后到达（或补发更正）的回执标记
  `late=true`，并给出 `impact_on_current_balance`：限额前后值、差额、未偿、
  超额与是否触发追索。短少确认按确认比例重算限额，超额自动冻结。
- **追索模型**：
  - 限额 = (剩余票面 × 承运人确认比例 − 退货) × 预付比例；回执到达前按临时
    比例（默认 0.7）融资，到达后切换正式比例（默认 0.8）。
  - 未偿 = 放款 − 回款；超额 = max(0, 未偿 − 限额)。
  - 先回款后退货：`cashback_due_from_seller` 标注应向出口商追回的已回款，
    累计 `cashback_claims_total = min(已回款, 退货)`。
  - 追索总额 = 未偿超额 + 对出口商现金追索；大于 0 即系统冻结。
- **发票拆分**：按比例生成子票，票面/数量/受让地位/冻结状态随之承继；此后
  两票独立计息与追踪；累计拆分比例不得超过 1。
- **币种换算**：汇率带生效日只增登记（`FX_RATE`），放款/回款/退货按 *业务
  发生日* 取当时生效的不可变汇率换算，所用汇率写入流水。
- **权限**：放款仅 `finance_manager`；冻结/解冻仅 `risk_officer`。解冻只清
  人工冻结，系统风险（重复质押/超额追索）未消除时仍保持冻结。

## 运行

```bash
python3 -m service.main          # 默认 data/ledger.jsonl，端口 8000
PORT=8000 LEDGER_PATH=/tmp/l.jsonl python3 -m service.main
```

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET  | `/health` | 健康检查 |
| POST | `/events` | 提交命令，角色由 `X-Role` 头指定 |
| GET  | `/invoices/<ref>` | 实时重算的台账视图（限额/已放款/未偿/可用/追索/冻结） |
| GET  | `/invoices/<ref>/journal` | 单票不可变流水 |
| GET  | `/journal` | 全部流水 |

## 提交示例

```bash
curl -X POST localhost:8000/events -H 'X-Role: operator' -H 'Content-Type: application/json' -d '{
  "event_id": "inv-1001",
  "occurred_at": "2026-09-01T09:00:00Z",
  "payload": {"type":"INVOICE","invoice_ref":"INV-1001","seller":"出口商甲",
              "debtor":"买方乙","currency":"CNY","face":"100000","qty":"100",
              "summary":"2026年9月灯具货款（原始摘要不可修改）"}
}'
```

命令 `type`：`INVOICE`、`SHIPMENT`、`CARRIER_RECEIPT`、`ASSIGNMENT_NOTICE`、
`SPLIT`、`LOAN`、`REPAYMENT`、`RETURN`、`FREEZE`、`UNFREEZE`、`FX_RATE`。
并发同票放款在存储单把锁内"折叠→校验→"入账"串行执行，超额请求得到 409 与完整
额度构成依据。

## 测试（异常演练）

```bash
python3 -m unittest discover -s tests -v
```

`tests/test_drills.py` 覆盖：8 线程并发同票融资（恰好 3 笔成功、5 笔带依据
拒绝）、保理商+银行重复质押、先回款后退货的现金追索、迟到短少回执触发超额
追索、拆分子票 + USD 放款按发生日汇率换算 + 部分回款、冻结/解冻授权、
断网补传与销毁内存后仅凭 JSONL 恢复且重放零新增流水。
