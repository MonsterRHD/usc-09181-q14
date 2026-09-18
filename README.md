# 贸易发票融资台账

为出口贸易保理/发票融资业务建立的**事件溯源（event-sourced）台账服务**：接收发票、
装运凭证、承运人回执、受让通知、放款、回款、退货、冻结等业务事件，按唯一业务标识
追踪每张发票的**可融资余额、已放款、在贷与追索状态**；重复质押、超额放款在受理时
被拒绝并给出可审计依据；服务崩溃/断网补传后重放同一流水得到完全一致的结果。

标准库实现，零第三方依赖，Python ≥ 3.11。

## 启动

```bash
LEDGER_PATH=data/ledger.jsonl PORT=8000 python3 -m service.main
# 或安装后：service
```

## 设计要点

| 需求 | 实现 |
| --- | --- |
| 原始凭证摘要不可修改 | 所有事件只增追加到 JSONL 流水，每条带 `seq` 与 SHA-256 哈希链；篡改任一字段在重放校验时即报 `CORRUPT_LOG` |
| 唯一业务标识 / 幂等 | 每条指令必须带 `event_id`；重复提交（断网补传、应用恢复）直接回放原结果，`duplicate=true`，绝不重复入账 |
| 可重算流水 | 台账状态**只能**由 `LedgerProjection.fold_event` 折叠事件得到；`/admin/rebuild` 全量重放，结果确定一致 |
| 重复质押识别 | 发票只能有一个受让方；再向其他保理商/银行发受让通知 → `DUPLICATE_PLEDGE`，并引用首次受让方与流水 seq |
| 拒绝超额放款并给依据 | 可融资余额 = 应收净额 × 融资比例 − 在贷；超额返回 `OVER_LIMIT` + `basis`（金额、退货、上限、在贷、算式、最近流水） |
| 发票拆分 | 子票金额合计必须等于母票应收净额；母票置 `SPLIT` 不得再融资，子票继承单据状态独立计额 |
| 部分回款 / 先回款后退货 | 回款冲减在贷本金；退货压低融资上限，在贷超上限部分自动形成 `recourse_due` 追索敞口，状态转 `RECOURSE` |
| 迟到承运人回执 | `late=true` 的回执单独标注其对当前余额的影响（`annotations`），补单后余额恢复有据可查 |
| 币种换算 | 放款前必须已登记汇率；放款事件内固化汇率快照与人民币金额，重放不依赖“当前汇率” |
| 冻结/解冻授权 | 仅 `risk_manager` 角色且 `confirmed=true` 显式确认方可执行；融资经理无权（`FORBIDDEN`） |
| 并发安全 | “校验+追加”在进程内 RLock + 跨进程 fcntl 文件锁内原子完成，并先追平其他进程的写入；并发同票融资只有一个成功 |
| 同一应收不得重复融资 | 全额回款结清后发票转 `SETTLED`，再次放款被拒 |

## 角色

- `ops`：登记发票、装运、回执、回款、退货
- `finance_manager`：放款、拆分、受让通知
- `risk_manager`：冻结 / 解除冻结（须显式确认）
- `treasury`：登记汇率

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/health` | 健康检查 |
| POST | `/events` | 提交一条指令（见下） |
| POST | `/events/batch` | 断网补传，JSON 数组，逐条幂等受理 |
| GET | `/invoices` | 全部发票台账快照 |
| GET | `/invoices/{id}` | 单票快照，含 `basis` 余额依据与 `annotations` 迟到凭证标注 |
| POST | `/admin/rebuild` | 从原始流水完整重算（服务恢复） |
| GET | `/admin/verify` | 校验哈希链完整性 |

### 指令类型（`POST /events` 的 `type`）

`INVOICE_REGISTERED`、`SHIPMENT_RECORDED`、`CARRIER_RECEIPT_RECORDED`、
`ASSIGNMENT_NOTIFIED`、`FX_RATE_REGISTERED`、`INVOICE_SPLIT`、
`FINANCING_APPROVED`、`REPAYMENT_RECORDED`、`RETURN_RECORDED`、
`FREEZE_APPLIED`、`FREEZE_RELEASED`。

每条指令公共字段：`event_id`（必填，唯一业务标识）、`type`、`actor`、`role`。

### 示例

```bash
# 建票（100000 CNY，融资比例 80%）
curl -s -X POST localhost:8000/events -H 'Content-Type: application/json' -d '{
  "event_id":"e1","type":"INVOICE_REGISTERED","actor":"u","role":"ops",
  "invoice_id":"INV-1","currency":"CNY","amount":"100000","advance_rate":"0.8"}'

# 齐套单据：装运、承运人回执、受让通知
# 迟到回执：在 CARRIER_RECEIPT_RECORDED 上加 "late": true 与 "impact"
# 放款（超过可融资余额 80000 即被拒，返回 basis 依据）
curl -s -X POST localhost:8000/events -H 'Content-Type: application/json' -d '{
  "event_id":"e5","type":"FINANCING_APPROVED","actor":"fm",
  "role":"finance_manager","invoice_id":"INV-1","amount":"90000"}'
# -> 422 OVER_LIMIT，basis 含：在贷 0 + 本次 90000 > 融资上限 80000
```

## 拒绝码

`OVER_LIMIT`（超额放款）、`DUPLICATE_PLEDGE`（重复质押）、
`DUPLICATE_NOTICE`（重复通知）、`DUPLICATE_INVOICE`、`DUPLICATE_DOC`、
`DOCS_INCOMPLETE`、`INVOICE_FROZEN`、`INVOICE_SPLIT`、`INVOICE_SETTLED`、
`NO_OUTSTANDING`、`RETURN_EXCEEDS_RECEIVABLE`、`SPLIT_AMOUNT_MISMATCH`、
`FX_RATE_MISSING`、`FORBIDDEN`、`CONFIRMATION_REQUIRED` 等。
被拒指令同样以 `REJECTED` 事件留痕（含原因与依据），事后可审计且重放稳定。

## 测试

```bash
python3 -W ignore -m unittest discover -s tests
```

覆盖：并发提交同票融资（单赢家）、先回款后退货触发追索、重复质押/重复通知、
幂等补传、拆分、外币换算与汇率快照、迟到回执标注、冻结授权、双进程共享流水、
服务恢复重放一致性、篡改原始凭证被拦截，共 30 个用例。

## 目录

- `service/store.py` —— 只增不改、哈希链、fsync、跨进程 fcntl 锁的 WAL 存储
- `service/domain.py` —— 发票状态机、余额/追索派生、事件折叠投影、币种换算
- `service/service.py` —— 指令校验、权限、幂等、并发原子受理、恢复重放
- `service/main.py` —— HTTP 入口（线程化标准库服务）
- `tests/` —— 领域/服务测试与含双进程演练的 HTTP 端到端测试
