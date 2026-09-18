"""台账领域与应用服务测试：覆盖异常演练全部场景。"""
import json
import os
import tempfile
import unittest
import warnings
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal

from service.domain import LedgerProjection
from service.service import LedgerService, CommandError
from service.store import WalStore, CorruptLogError

warnings.simplefilter("ignore", ResourceWarning)  # 临时锁文件随进程退出回收


def new_service(path=None):
    fd, tmp = tempfile.mkstemp(suffix=".jsonl")
    os.close(fd)
    os.unlink(tmp)  # WalStore 会自行创建
    svc = LedgerService(WalStore(tmp))
    return svc, tmp


def cleanup(service, path):
    service.store.close()
    for suffix in ("", ".lock"):
        try:
            os.unlink(path + suffix)
        except OSError:
            pass


def base_invoice(svc, iid="INV-1", amount="100000", currency="CNY",
                 rate="0.8"):
    r = svc.submit({"event_id": f"reg-{iid}", "type": "INVOICE_REGISTERED",
                    "actor": "u1", "role": "ops",
                    "invoice_id": iid, "currency": currency,
                    "amount": amount, "advance_rate": rate})
    assert r["accepted"], r


def complete_docs(svc, iid="INV-1", assignee="FACTOR-A", prefix="d"):
    for k, etype, extra in [
        ("ship", "SHIPMENT_RECORDED", {}),
        ("rcpt", "CARRIER_RECEIPT_RECORDED", {}),
        ("asn", "ASSIGNMENT_NOTIFIED", {"assignee": assignee}),
    ]:
        cmd = {"event_id": f"{prefix}-{iid}-{k}", "type": etype,
               "actor": "u1", "role": "ops", "invoice_id": iid}
        cmd.update(extra)
        r = svc.submit(cmd)
        assert r["accepted"], r


class FinancingRulesTest(unittest.TestCase):
    def setUp(self):
        self.svc, self.path = new_service()
        base_invoice(self.svc)

    def test_full_flow_and_balance_basis(self):
        complete_docs(self.svc)
        snap = self.svc.invoice("INV-1")
        self.assertTrue(snap["docs_complete"])
        self.assertEqual(snap["available"], "80000.00")
        self.assertEqual(snap["status"], "ACTIVE")

        r = self.svc.submit({"event_id": "adv-1", "type": "FINANCING_APPROVED",
                             "actor": "fm", "role": "finance_manager",
                             "invoice_id": "INV-1", "amount": "50000"})
        self.assertTrue(r["accepted"], r)
        snap = self.svc.invoice("INV-1")
        self.assertEqual(snap["advances"], "50000.00")
        self.assertEqual(snap["outstanding"], "50000.00")
        self.assertEqual(snap["available"], "30000.00")

    def test_reject_over_limit_with_basis(self):
        complete_docs(self.svc)
        r = self.svc.submit({"event_id": "adv-big", "type": "FINANCING_APPROVED",
                             "actor": "fm", "role": "finance_manager",
                             "invoice_id": "INV-1", "amount": "90000"})
        self.assertFalse(r["accepted"])
        self.assertEqual(r["code"], "OVER_LIMIT")
        # 依据必须能说清拒绝理由
        self.assertTrue(any("融资上限" in b for b in r["basis"]))
        self.assertTrue(any("90000.00" in b and "80000.00" in b for b in r["basis"]))
        snap = self.svc.invoice("INV-1")
        self.assertEqual(snap["advances"], "0.00")  # 拒绝不得产生余额影响

    def test_docs_incomplete_blocks_financing(self):
        r = self.svc.submit({"event_id": "adv-nodoc", "type": "FINANCING_APPROVED",
                             "actor": "fm", "role": "finance_manager",
                             "invoice_id": "INV-1", "amount": "10000"})
        self.assertFalse(r["accepted"])
        self.assertEqual(r["code"], "DOCS_INCOMPLETE")

    def test_role_required_for_financing(self):
        complete_docs(self.svc)
        r = self.svc.submit({"event_id": "adv-role", "type": "FINANCING_APPROVED",
                             "actor": "x", "role": "ops",
                             "invoice_id": "INV-1", "amount": "10000"})
        self.assertFalse(r["accepted"])
        self.assertEqual(r["code"], "FORBIDDEN")


class DuplicatePledgeTest(unittest.TestCase):
    def setUp(self):
        self.svc, _ = new_service()
        base_invoice(self.svc)
        complete_docs(self.svc, assignee="FACTOR-A")

    def test_same_invoice_to_two_financiers_rejected(self):
        # 出口商又把同一批发票转让给银行
        r = self.svc.submit({"event_id": "asn-bank", "type": "ASSIGNMENT_NOTIFIED",
                             "actor": "u1", "role": "ops",
                             "invoice_id": "INV-1", "assignee": "BANK-B"})
        self.assertFalse(r["accepted"])
        self.assertEqual(r["code"], "DUPLICATE_PLEDGE")
        self.assertTrue(any("FACTOR-A" in b for b in r["basis"]))
        snap = self.svc.invoice("INV-1")
        self.assertEqual(snap["assignee"], "FACTOR-A")

    def test_duplicate_notice_same_assignee_is_idempotent(self):
        r = self.svc.submit({"event_id": "asn-again", "type": "ASSIGNMENT_NOTIFIED",
                             "actor": "u1", "role": "ops",
                             "invoice_id": "INV-1", "assignee": "FACTOR-A"})
        self.assertFalse(r["accepted"])
        self.assertEqual(r["code"], "DUPLICATE_NOTICE")

    def test_duplicate_invoice_registration(self):
        r = self.svc.submit({"event_id": "reg-dup", "type": "INVOICE_REGISTERED",
                             "actor": "u1", "role": "ops",
                             "invoice_id": "INV-1", "currency": "CNY",
                             "amount": "100000"})
        self.assertFalse(r["accepted"])
        self.assertEqual(r["code"], "DUPLICATE_INVOICE")


class IdempotencyTest(unittest.TestCase):
    def setUp(self):
        self.svc, _ = new_service()
        base_invoice(self.svc)
        complete_docs(self.svc)

    def test_same_event_id_does_not_double_advance(self):
        cmd = {"event_id": "adv-idem", "type": "FINANCING_APPROVED",
               "actor": "fm", "role": "finance_manager",
               "invoice_id": "INV-1", "amount": "70000"}
        first = self.svc.submit(dict(cmd))
        again = self.svc.submit(dict(cmd))
        third = self.svc.submit(dict(cmd))
        self.assertTrue(first["accepted"])
        self.assertTrue(again["accepted"])
        self.assertTrue(third["accepted"])
        self.assertTrue(again["duplicate"] and third["duplicate"])
        self.assertEqual(first["seq"], again["seq"])
        snap = self.svc.invoice("INV-1")
        self.assertEqual(snap["advances"], "70000.00")  # 只放了一次

    def test_rejected_command_replay_is_stable(self):
        bad = {"event_id": "adv-bad", "type": "FINANCING_APPROVED",
               "actor": "fm", "role": "finance_manager",
               "invoice_id": "INV-1", "amount": "99000"}
        r1 = self.svc.submit(dict(bad))
        r2 = self.svc.submit(dict(bad))
        self.assertFalse(r1["accepted"])
        self.assertFalse(r2["accepted"])
        self.assertTrue(r2["duplicate"])
        self.assertEqual(r1["code"], r2["code"])

    def test_batch_backfill_is_idempotent(self):
        cmds = [
            {"event_id": "b1", "type": "FINANCING_APPROVED", "actor": "fm",
             "role": "finance_manager", "invoice_id": "INV-1", "amount": "10000"},
            {"event_id": "b2", "type": "FINANCING_APPROVED", "actor": "fm",
             "role": "finance_manager", "invoice_id": "INV-1", "amount": "10000"},
        ]
        out1 = self.svc.submit_batch([dict(c) for c in cmds])
        out2 = self.svc.submit_batch([dict(c) for c in cmds])
        self.assertEqual(out1["count"], 2)
        self.assertTrue(all(x["accepted"] for x in out1["results"]))
        self.assertTrue(all(x["duplicate"] for x in out2["results"]))
        self.assertEqual(self.svc.invoice("INV-1")["advances"], "20000.00")


class ConcurrentFinancingTest(unittest.TestCase):
    def setUp(self):
        self.svc, _ = new_service()
        base_invoice(self.svc)
        complete_docs(self.svc)

    def test_concurrent_claims_on_same_invoice(self):
        # 两个融资渠道并发就同票申请全额放款，只能有一个成功
        def claim(tag):
            return self.svc.submit({
                "event_id": f"adv-{tag}", "type": "FINANCING_APPROVED",
                "actor": "fm", "role": "finance_manager",
                "invoice_id": "INV-1", "amount": "80000"})

        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(claim, [f"t{i}" for i in range(8)]))
        accepted = [r for r in results if r["accepted"]]
        rejected = [r for r in results if not r["accepted"]]
        self.assertEqual(len(accepted), 1)
        self.assertTrue(all(r["code"] == "OVER_LIMIT" for r in rejected))
        self.assertEqual(self.svc.invoice("INV-1")["outstanding"], "80000.00")
        # 被拒的申请同样留痕，事后可审计
        chain = self.svc.store.read_all()
        self.assertGreaterEqual(
            sum(1 for r in chain if r["type"] == "REJECTED"), 7)

    def test_concurrent_same_event_id_collapses_to_one(self):
        cmd = {"event_id": "adv-same", "type": "FINANCING_APPROVED",
               "actor": "fm", "role": "finance_manager",
               "invoice_id": "INV-1", "amount": "80000"}
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(
                lambda i: self.svc.submit(dict(cmd)), range(8)))
        seqs = {r["seq"] for r in results}
        self.assertEqual(len(seqs), 1)
        self.assertEqual(self.svc.invoice("INV-1")["outstanding"], "80000.00")


class RepaymentReturnTest(unittest.TestCase):
    def setUp(self):
        self.svc, _ = new_service()
        base_invoice(self.svc)
        complete_docs(self.svc)
        self.assertTrue(self.svc.submit({
            "event_id": "adv", "type": "FINANCING_APPROVED", "actor": "fm",
            "role": "finance_manager", "invoice_id": "INV-1",
            "amount": "80000"})["accepted"])

    def test_partial_repayment_then_return_triggers_recourse(self):
        # 先部分回款 30000，在贷 50000
        r = self.svc.submit({"event_id": "rep1", "type": "REPAYMENT_RECORDED",
                             "actor": "u", "role": "ops",
                             "invoice_id": "INV-1", "amount": "30000"})
        self.assertTrue(r["accepted"], r)
        snap = self.svc.invoice("INV-1")
        self.assertEqual(snap["outstanding"], "50000.00")

        # 后退货 50000：应收净额降到 50000，融资上限降到 40000 -> 追索 10000
        r = self.svc.submit({"event_id": "ret1", "type": "RETURN_RECORDED",
                             "actor": "u", "role": "ops",
                             "invoice_id": "INV-1", "amount": "50000"})
        self.assertTrue(r["accepted"], r)
        snap = self.svc.invoice("INV-1")
        self.assertEqual(snap["status"], "RECOURSE")
        self.assertEqual(snap["recourse_due"], "10000.00")
        self.assertEqual(snap["available"], "0.00")

    def test_cannot_borrow_again_after_full_repayment(self):
        # 全额放款后全额回款，发票结清；不得就同一应收再次放款
        r = self.svc.submit({"event_id": "rep-full", "type": "REPAYMENT_RECORDED",
                             "actor": "u", "role": "ops",
                             "invoice_id": "INV-1", "amount": "80000"})
        self.assertTrue(r["accepted"], r)
        snap = self.svc.invoice("INV-1")
        self.assertEqual(snap["status"], "SETTLED")
        self.assertEqual(snap["available"], "0.00")
        r = self.svc.submit({"event_id": "adv-again",
                             "type": "FINANCING_APPROVED", "actor": "fm",
                             "role": "finance_manager", "invoice_id": "INV-1",
                             "amount": "1000"})
        self.assertFalse(r["accepted"])
        self.assertEqual(r["code"], "INVOICE_SETTLED")

    def test_repayment_without_outstanding_rejected(self):
        svc2, _ = new_service()
        base_invoice(svc2, "INV-X")
        complete_docs(svc2, "INV-X", prefix="dx")
        r = svc2.submit({"event_id": "rep-x", "type": "REPAYMENT_RECORDED",
                         "actor": "u", "role": "ops",
                         "invoice_id": "INV-X", "amount": "100"})
        self.assertFalse(r["accepted"])
        self.assertEqual(r["code"], "NO_OUTSTANDING")


class FreezeTest(unittest.TestCase):
    def setUp(self):
        self.svc, _ = new_service()
        base_invoice(self.svc)
        complete_docs(self.svc)

    def test_only_risk_role_with_confirmation(self):
        # 融资经理无权冻结
        r = self.svc.submit({"event_id": "fz-fm", "type": "FREEZE_APPLIED",
                             "actor": "fm", "role": "finance_manager",
                             "invoice_id": "INV-1", "reason": "争议",
                             "confirmed": True})
        self.assertEqual(r["code"], "FORBIDDEN")

        # 风控未显式确认 -> 拒绝
        r = self.svc.submit({"event_id": "fz-nc", "type": "FREEZE_APPLIED",
                             "actor": "rm", "role": "risk_manager",
                             "invoice_id": "INV-1", "reason": "争议"})
        self.assertEqual(r["code"], "CONFIRMATION_REQUIRED")

        # 风控确认 -> 冻结生效，放款被拒
        r = self.svc.submit({"event_id": "fz-ok", "type": "FREEZE_APPLIED",
                             "actor": "rm", "role": "risk_manager",
                             "invoice_id": "INV-1", "reason": "买方争议",
                             "confirmed": True})
        self.assertTrue(r["accepted"], r)
        snap = self.svc.invoice("INV-1")
        self.assertTrue(snap["frozen"])
        self.assertEqual(snap["status"], "FROZEN")

        r = self.svc.submit({"event_id": "adv-fz", "type": "FINANCING_APPROVED",
                             "actor": "fm", "role": "finance_manager",
                             "invoice_id": "INV-1", "amount": "1000"})
        self.assertEqual(r["code"], "INVOICE_FROZEN")

        # 解除冻结同样需要风控确认
        r = self.svc.submit({"event_id": "rel-ok", "type": "FREEZE_RELEASED",
                             "actor": "rm", "role": "risk_manager",
                             "invoice_id": "INV-1", "confirmed": True})
        self.assertTrue(r["accepted"], r)
        self.assertFalse(self.svc.invoice("INV-1")["frozen"])


class CurrencyTest(unittest.TestCase):
    def setUp(self):
        self.svc, _ = new_service()
        base_invoice(self.svc, iid="INV-U", currency="USD", amount="100000")

    def test_fx_required_and_snapshot_in_event(self):
        complete_docs(self.svc, "INV-U", prefix="du")
        # 未登记汇率 -> 拒绝放款
        r = self.svc.submit({"event_id": "adv-usd-nofx",
                             "type": "FINANCING_APPROVED", "actor": "fm",
                             "role": "finance_manager", "invoice_id": "INV-U",
                             "amount": "80000"})
        self.assertEqual(r["code"], "FX_RATE_MISSING")

        r = self.svc.submit({"event_id": "fx-usd", "type": "FX_RATE_REGISTERED",
                             "actor": "t", "role": "treasury",
                             "currency": "USD", "rate_to_cny": "7.250000",
                             "as_of": "2026-01-01T00:00:00Z"})
        self.assertTrue(r["accepted"], r)
        r = self.svc.submit({"event_id": "adv-usd", "type": "FINANCING_APPROVED",
                             "actor": "fm", "role": "finance_manager",
                             "invoice_id": "INV-U", "amount": "80000"})
        self.assertTrue(r["accepted"], r)
        rec = self.svc._records[r["seq"]]
        self.assertEqual(rec["payload"]["amount_cny"], "580000.00")
        self.assertEqual(rec["payload"]["fx_rate_to_cny"], "7.250000")
        snap = self.svc.invoice("INV-U")
        self.assertEqual(snap["outstanding_cny"], "580000.00")

    def test_replay_uses_fx_snapshot_not_current_rate(self):
        # 登记 7.25，放款，之后汇率变为 7.10，重放结果仍按 7.25
        complete_docs(self.svc, "INV-U", prefix="du2")
        self.svc.submit({"event_id": "fx-1", "type": "FX_RATE_REGISTERED",
                         "actor": "t", "role": "treasury", "currency": "USD",
                         "rate_to_cny": "7.250000",
                         "as_of": "2026-01-01T00:00:00Z"})
        self.svc.submit({"event_id": "adv-u", "type": "FINANCING_APPROVED",
                         "actor": "fm", "role": "finance_manager",
                         "invoice_id": "INV-U", "amount": "80000"})
        self.svc.submit({"event_id": "fx-2", "type": "FX_RATE_REGISTERED",
                         "actor": "t", "role": "treasury", "currency": "USD",
                         "rate_to_cny": "7.100000",
                         "as_of": "2026-09-01T00:00:00Z"})
        rebuilt = LedgerService(WalStore(self.svc.store.path))
        rec = [r_ for r_ in rebuilt.store.read_all()
               if r_["type"] == "FINANCING_APPROVED"][0]
        self.assertEqual(rec["payload"]["amount_cny"], "580000.00")


class SplitTest(unittest.TestCase):
    def setUp(self):
        self.svc, _ = new_service()
        base_invoice(self.svc)
        complete_docs(self.svc)

    def test_split_then_children_finance(self):
        r = self.svc.submit({
            "event_id": "split-1", "type": "INVOICE_SPLIT", "actor": "fm",
            "role": "finance_manager", "invoice_id": "INV-1",
            "children": [{"invoice_id": "INV-1A", "amount": "40000"},
                         {"invoice_id": "INV-1B", "amount": "60000"}]})
        self.assertTrue(r["accepted"], r)
        parent = self.svc.invoice("INV-1")
        self.assertEqual(parent["status"], "SPLIT")
        self.assertEqual(parent["available"], "0.00")

        # 母票不得再放款
        r = self.svc.submit({"event_id": "adv-parent",
                             "type": "FINANCING_APPROVED", "actor": "fm",
                             "role": "finance_manager", "invoice_id": "INV-1",
                             "amount": "1000"})
        self.assertEqual(r["code"], "INVOICE_SPLIT")

        # 子票继承单据状态，可独立融资，各自上限独立
        snap_b = self.svc.invoice("INV-1B")
        self.assertTrue(snap_b["docs_complete"])
        self.assertEqual(snap_b["finance_limit"], "48000.00")
        r = self.svc.submit({"event_id": "adv-b", "type": "FINANCING_APPROVED",
                             "actor": "fm", "role": "finance_manager",
                             "invoice_id": "INV-1B", "amount": "48000"})
        self.assertTrue(r["accepted"], r)
        r = self.svc.submit({"event_id": "adv-b2", "type": "FINANCING_APPROVED",
                             "actor": "fm", "role": "finance_manager",
                             "invoice_id": "INV-1B", "amount": "1"})
        self.assertEqual(r["code"], "OVER_LIMIT")

    def test_split_amounts_must_tie_out(self):
        r = self.svc.submit({
            "event_id": "split-bad", "type": "INVOICE_SPLIT", "actor": "fm",
            "role": "finance_manager", "invoice_id": "INV-1",
            "children": [{"invoice_id": "X1", "amount": "40000"},
                         {"invoice_id": "X2", "amount": "50000"}]})
        self.assertEqual(r["code"], "SPLIT_AMOUNT_MISMATCH")


class LateCarrierReceiptTest(unittest.TestCase):
    def test_late_receipt_annotated_with_balance_impact(self):
        svc, _ = new_service()
        base_invoice(svc)
        # 只有发票+装运+受让，缺承运人回执 -> 不可融资
        svc.submit({"event_id": "s1", "type": "SHIPMENT_RECORDED",
                    "actor": "u", "role": "ops", "invoice_id": "INV-1"})
        svc.submit({"event_id": "a1", "type": "ASSIGNMENT_NOTIFIED",
                    "actor": "u", "role": "ops", "invoice_id": "INV-1",
                    "assignee": "FACTOR-A"})
        blocked = svc.submit({"event_id": "adv-early",
                              "type": "FINANCING_APPROVED", "actor": "fm",
                              "role": "finance_manager", "invoice_id": "INV-1",
                              "amount": "10000"})
        self.assertEqual(blocked["code"], "DOCS_INCOMPLETE")

        # 迟到的承运人回执到达，显式标注对当前余额的影响
        r = svc.submit({
            "event_id": "late-rcpt", "type": "CARRIER_RECEIPT_RECORDED",
            "actor": "u", "role": "ops", "invoice_id": "INV-1",
            "late": True, "recorded_at": "2026-03-10T08:00:00Z",
            "expected_by": "2026-03-01T00:00:00Z",
            "note": "承运人系统故障，回执延迟 9 天",
            "impact": "补单后单据齐套，可融资余额由 0 恢复为 80000.00 CNY"})
        self.assertTrue(r["accepted"], r)
        snap = svc.invoice("INV-1")
        self.assertTrue(snap["docs_complete"])
        self.assertEqual(snap["available"], "80000.00")
        self.assertEqual(len(snap["annotations"]), 1)
        note = snap["annotations"][0]
        self.assertTrue(note["late"])
        self.assertIn("80000.00", note["impact"])


class RecoveryAndTamperTest(unittest.TestCase):
    def test_rebuild_after_restart_matches(self):
        svc, path = new_service()
        base_invoice(svc)
        complete_docs(svc)
        svc.submit({"event_id": "adv", "type": "FINANCING_APPROVED",
                    "actor": "fm", "role": "finance_manager",
                    "invoice_id": "INV-1", "amount": "60000"})
        svc.submit({"event_id": "rep", "type": "REPAYMENT_RECORDED",
                    "actor": "u", "role": "ops", "invoice_id": "INV-1",
                    "amount": "20000"})
        before = svc.invoice("INV-1")

        # 模拟服务恢复：新建实例完整重放
        revived = LedgerService(WalStore(path))
        after = revived.invoice("INV-1")
        self.assertEqual(before, after)
        info = revived.rebuild()
        self.assertEqual(info["last_seq"], before["last_seq"])

    def test_replay_is_deterministic(self):
        svc, path = new_service()
        base_invoice(svc, "INV-D")
        complete_docs(svc, "INV-D", prefix="dd")
        svc.submit({"event_id": "adv-d", "type": "FINANCING_APPROVED",
                    "actor": "fm", "role": "finance_manager",
                    "invoice_id": "INV-D", "amount": "30000"})
        s1 = LedgerService(WalStore(path)).invoice("INV-D")
        s2 = LedgerService(WalStore(path)).invoice("INV-D")
        self.assertEqual(s1, s2)

    def test_tampered_voucher_detected(self):
        svc, path = new_service()
        base_invoice(svc)
        # 直接篡改原始凭证摘要（把金额 100000 改成 999999）
        with open(path, "r", encoding="utf-8") as fh:
            lines = fh.readlines()
        obj = json.loads(lines[0])
        obj["payload"]["amount"] = "999999.00"
        lines[0] = json.dumps(obj, ensure_ascii=False) + "\n"
        with open(path, "w", encoding="utf-8") as fh:
            fh.writelines(lines)
        with self.assertRaises(CorruptLogError):
            LedgerService(WalStore(path))
        with self.assertRaises(CorruptLogError):
            svc.verify_chain()


if __name__ == "__main__":
    unittest.main()
