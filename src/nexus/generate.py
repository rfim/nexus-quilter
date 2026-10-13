"""Synthetic wealth-platform feeds. Every client, colleague and fund here is invented.

Writes newline-delimited JSON to landing/<feed>/, the shape a platform CDC stream,
a fund-manager file drop or an HR extract would land in. A handful of malformed and
replayed records are included on purpose, so quarantine and idempotence have
something real to do.
"""
import json
import random
from datetime import date, datetime, timedelta
from pathlib import Path

from nexus.spark import ROOT

LANDING = ROOT / "landing"
START, END = date(2025, 10, 1), date(2026, 9, 30)
WRAPPERS = ["ISA", "PENSION", "GIA", "JISA", "BOND"]
WRAPPER_WEIGHTS = [34, 38, 18, 4, 6]
CHANNELS = ["Quilter adviser", "Independent adviser", "Direct"]
FUNCTIONS = ["Advice", "Investments", "Operations", "Technology", "Distribution"]
FIRST = ["Amara", "Ben", "Chloe", "Dev", "Ella", "Femi", "Grace", "Hugo", "Isla", "Jai", "Kira", "Leo"]
LAST = ["Ashford", "Bennett", "Clarke", "Dhillon", "Evans", "Fraser", "Grant", "Hale", "Iqbal", "Jones"]


def _isin(rng: random.Random) -> str:
    return "GB00Q" + "".join(rng.choice("ABCDEFGHJKLMNPQRSTUVWXYZ0123456789") for _ in range(6)) + str(rng.randint(0, 9))


def _days(a: date, b: date):
    d = a
    while d <= b:
        yield d
        d += timedelta(days=1)


def _write(feed: str, rows: list[dict]) -> None:
    path = LANDING / feed
    path.mkdir(parents=True, exist_ok=True)
    with open(path / "part-0001.json", "w") as f:
        for r in rows:
            f.write(json.dumps(r, default=str) + "\n")


def generate(seed: int = 7) -> dict:
    rng = random.Random(seed)

    # funds: share classes with a charge history; three are repriced mid-year (SCD2 work)
    funds, fund_history = [], []
    for k in range(12):
        isin = _isin(rng)
        ocf = round(rng.uniform(0.15, 0.95), 2)
        funds.append({"isin": isin, "fund_name": f"Nexus Portfolio {k + 1:02d}",
                      "asset_class": rng.choice(["Multi-asset", "Equity", "Bond", "Cash"])})
        fund_history.append({"isin": isin, "ongoing_charge_pct": ocf, "risk_rating": rng.randint(2, 6),
                             "effective_from": "2025-01-01"})
        if k in (2, 5, 9):
            fund_history.append({"isin": isin, "ongoing_charge_pct": round(ocf - 0.08, 2),
                                 "risk_rating": fund_history[-1]["risk_rating"], "effective_from": "2026-04-01"})

    # clients and accounts
    clients, accounts = [], []
    for c in range(2400):
        joined = START - timedelta(days=rng.randint(0, 900)) if rng.random() < 0.8 else \
            START + timedelta(days=rng.randint(0, (END - START).days))
        entity = "hnw" if rng.random() < 0.14 else "affluent"
        clients.append({"client_id": f"C{c:05d}", "legal_entity": entity,
                        "channel": rng.choices(CHANNELS, [55, 33, 12])[0], "joined_on": joined,
                        "email": f"client{c:05d}@example.com",
                        "national_insurance_no": f"QQ{rng.randint(100000, 999999)}C",
                        "vulnerable": rng.random() < 0.06})
        for w in rng.sample(WRAPPERS, k=rng.choice([1, 1, 2])):
            accounts.append({"account_id": f"A{len(accounts):06d}", "client_id": f"C{c:05d}",
                             "wrapper_code": w, "opened_on": joined,
                             "opening_value": round(rng.lognormvariate(10.8 if entity == "affluent" else 12.6, 0.7), 2)})

    # transactions: the platform CDC stream
    tx, n = [], 0
    for a in accounts:
        opened = date.fromisoformat(str(a["opened_on"]))
        for d in _days(max(opened, START), END):
            if rng.random() < 0.012:
                ttype = rng.choices(["contribution", "withdrawal", "switch_in", "switch_out", "fee", "income"],
                                    [44, 16, 10, 10, 14, 6])[0]
                amt = round(rng.lognormvariate(7.4, 1.0), 2)
                sign = -1 if ttype in ("withdrawal", "switch_out", "fee") else 1
                if ttype == "fee":
                    amt = round(amt * 0.02, 2)
                n += 1
                tx.append({"transaction_id": f"T{n:07d}", "account_id": a["account_id"],
                           "isin": rng.choice(funds)["isin"], "transaction_type": ttype,
                           "amount_gbp": sign * amt, "trade_date": d, "cdc_lsn": n * 10, "op": "I",
                           "wrapper_code": a["wrapper_code"]})
    # replayed offsets (same lsn, delivered twice) and a later correction (higher lsn)
    replays = [dict(t) for t in rng.sample(tx, 180)]
    originals = rng.sample(tx, 40)
    corrections = [dict(t, amount_gbp=round(t["amount_gbp"] * 1.01, 2), cdc_lsn=t["cdc_lsn"] + 1, op="U")
                   for t in originals]
    malformed = [dict(rng.choice(tx), transaction_id=f"X{k:05d}", isin="BADISIN") for k in range(9)] + \
                [dict(rng.choice(tx), transaction_id=f"Y{k:05d}", transaction_type="unknown") for k in range(6)]
    stream = tx + replays + corrections + malformed
    rng.shuffle(stream)

    # finance ledger: what the general ledger booked per day, independently of the platform
    ledger: dict[tuple, float] = {}
    flow_types = ("contribution", "withdrawal", "fee")   # the Net Flows definition, agreed with Finance
    for t in tx:
        if t["transaction_type"] not in flow_types:
            continue
        key = (str(t["trade_date"]), t["wrapper_code"])
        ledger[key] = ledger.get(key, 0.0) + t["amount_gbp"]
    for o, c in zip(originals, corrections):
        if o["transaction_type"] not in flow_types:
            continue
        key = (str(o["trade_date"]), o["wrapper_code"])
        ledger[key] += c["amount_gbp"] - o["amount_gbp"]
    # one posting the platform never received: the reconciliation should catch exactly this
    ledger[("2026-08-14", "ISA")] = ledger.get(("2026-08-14", "ISA"), 0.0) + 250.00
    ledger_rows = [{"trade_date": k[0], "wrapper_code": k[1], "ledger_net_flows_gbp": round(v, 2)}
                   for k, v in sorted(ledger.items())]

    # service journeys (Consumer Duty: can clients leave as easily as they join?)
    journeys = []
    for j in range(3200):
        kind = rng.choices(["transfer_in", "transfer_out", "withdrawal", "switch"], [30, 18, 34, 18])[0]
        created = START + timedelta(days=rng.randint(0, (END - START).days - 20))
        base = {"transfer_in": 9, "transfer_out": 13, "withdrawal": 3, "switch": 2}[kind]
        done = None if rng.random() < 0.04 else created + timedelta(days=max(1, int(rng.gammavariate(2.2, base / 2.2))))
        journeys.append({"journey_id": f"J{j:05d}", "client_id": rng.choice(clients)["client_id"],
                         "journey_type": kind, "created_on": created, "completed_on": done})

    # colleagues: HR extract (names are personal data and are masked downstream)
    colleagues = []
    for e in range(1100):
        hired = date(2018, 1, 1) + timedelta(days=rng.randint(0, 3150))
        left = hired + timedelta(days=rng.randint(150, 1700)) if rng.random() < 0.5 else None
        colleagues.append({"colleague_id": f"E{e:04d}", "full_name": f"{rng.choice(FIRST)} {rng.choice(LAST)}",
                           "function": rng.choices(FUNCTIONS, [30, 14, 26, 18, 12])[0],
                           "hired_on": hired, "left_on": left if left and left <= END else None})

    # marketing spend per channel and month
    spend = []
    m = date(START.year, START.month, 1)
    while m <= END:
        for ch, base in zip(CHANNELS, [42000, 31000, 18000]):
            spend.append({"month": m, "channel": ch, "spend_gbp": round(base * rng.uniform(0.8, 1.25), 2)})
        m = date(m.year + (m.month == 12), m.month % 12 + 1, 1)

    feeds = {"funds": funds, "fund_charges": fund_history, "clients": clients, "accounts": accounts,
             "platform_transactions": stream, "finance_ledger": ledger_rows, "service_journeys": journeys,
             "colleagues": colleagues, "marketing_spend": spend}
    for name, rows in feeds.items():
        _write(name, rows)
    return {name: len(rows) for name, rows in feeds.items()} | {
        "replayed_records": len(replays), "corrections": len(corrections), "malformed_records": len(malformed),
        "generated_at": datetime(2026, 10, 1, 6, 0).isoformat()}
