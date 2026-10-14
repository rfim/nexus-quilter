"""End to end: land -> bronze -> silver -> gold -> contracts, then a late batch of corrections.

The same entry point a Databricks job calls. Every step writes Delta, so the run leaves a real
transaction log behind: replays, MERGEs, overwrites and the late restatement are all in the history.
"""
import json
import time

from nexus import bronze, contracts, gold, silver
from nexus.generate import generate
from nexus.spark import TARGET, get_spark, table


class Steps:
    """Records each task's duration, like a Workflows run."""

    def __init__(self):
        self.tasks, self.t0 = [], time.time()

    def run(self, key: str, label: str, fn, *args):
        start = time.time()
        out = fn(*args)
        self.tasks.append({"key": key, "label": label, "start_s": round(start - self.t0, 1),
                           "duration_s": round(time.time() - start, 1)})
        return out


def _transform(spark, tx_contract):
    valid, quarantined = silver.split_by_contract(spark, tx_contract)
    silver.build_dim_fund_share_class(spark)
    silver.build_conformed(spark)
    return valid, quarantined


def _publish(spark):
    gold.build_fct_platform_transaction(spark)
    for build in gold.PRODUCTS:
        build(spark)


def _check_all(spark):
    return [{"product": c["product"], "department": c["department"],
             "results": (res := contracts.run_checks(spark, c)),
             "passed": sum(r["status"] == "pass" for r in res), "total": len(res)}
            for c in contracts.load_products()]


def _version(spark, layer, name) -> int:
    return spark.sql(f"DESCRIBE HISTORY {table(layer, name)}").agg({"version": "max"}).first()[0]


def run() -> dict:
    steps = Steps()
    spark = get_spark()
    log = {"feeds": steps.run("generate", "Generate synthetic feeds", generate)}

    sources = contracts.load_sources()
    log["bronze"] = steps.run("ingest", "Ingest feeds to bronze (MERGE)",
                              lambda: {s["feed"]: bronze.ingest(spark, s) for s in sources})
    # replay every feed: idempotence means nothing changes
    log["bronze_after_replay"] = steps.run("replay", "Replay every feed (should change nothing)",
                                           lambda: {s["feed"]: bronze.ingest(spark, s) for s in sources})

    tx_contract = next(s for s in sources if s["feed"] == "platform_transactions")
    valid, quarantined = steps.run("silver", "Quality split, SCD2 and conformed silver", _transform, spark, tx_contract)
    log["silver"] = {"platform_transaction": valid, "quarantine": quarantined,
                     "quarantine_reasons": {r["rule"]: r["n"] for r in spark.sql(
                         f"SELECT explode(_failed_rules) AS rule, count(*) AS n FROM "
                         f"{table('silver', 'platform_transaction_quarantine')} GROUP BY 1").collect()}}

    steps.run("gold", "Fact (incremental MERGE) and department products", _publish, spark)
    first = spark.table(table("gold", "fct_platform_transaction")).count()
    steps.run("rerun", "Re-run the fact inside the lookback window", gold.build_fct_platform_transaction, spark)
    log["gold_fact"] = {"rows": first, "rows_after_rerun": spark.table(table("gold", "fct_platform_transaction")).count()}

    issued = steps.run("check_issued", "Contract checks on the first publish", _check_all, spark)
    log["as_issued"] = {"finance_version": _version(spark, "gold", "finance_net_flows_daily"),
                        "contracts": [{k: c[k] for k in ("product", "department", "passed", "total")} | {
                            "failing": [r["check"] for r in c["results"] if r["status"] == "fail"]} for c in issued]}

    # the late batch: 40 corrections the platform sends a day later (higher CDC sequence wins)
    late = dict(tx_contract, source=dict(tx_contract["source"], feed="platform_transactions_late"))
    log["late_batch"] = {"rows": steps.run("late", "Late corrections batch to bronze (MERGE)", bronze.ingest, spark, late)}
    steps.run("restate", "Re-run silver and gold with the corrections", lambda: (_transform(spark, tx_contract), _publish(spark)))

    compiled = TARGET / "compiled"
    compiled.mkdir(parents=True, exist_ok=True)
    for c in contracts.load_products():
        (compiled / f"{c['product']}_expectations.py").write_text(contracts.compile_expectations(c))
        (compiled / f"{c['product']}_governance.sql").write_text(contracts.compile_governance(c))
    log["contracts"] = steps.run("check", "Contract checks on the restated products", _check_all, spark)
    log["current"] = {"finance_version": _version(spark, "gold", "finance_net_flows_daily")}
    log["tasks"] = steps.tasks
    log["seconds"] = round(time.time() - steps.t0, 1)
    (TARGET / "run_log.json").write_text(json.dumps(log, indent=2, default=str))
    return log


def summarise(out: dict) -> None:
    print(json.dumps({k: v for k, v in out.items() if k not in ("contracts", "tasks")}, indent=2, default=str))
    for c in out["contracts"]:
        print(f"{c['department']:<10} {c['product']:<32} {c['passed']}/{c['total']} checks passed")
        for r in c["results"]:
            if r["status"] == "fail":
                print(f"           FAIL {r['check']}: {r['failing_rows']} row(s) {r['sample'][:1]}")


if __name__ == "__main__":
    summarise(run())
