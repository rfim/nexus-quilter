"""End to end: land -> bronze -> silver -> gold -> contracts. The same entry point a Databricks job calls."""
import json
import time

from nexus import bronze, contracts, gold, silver
from nexus.generate import generate
from nexus.spark import TARGET, get_spark, table


def run() -> dict:
    t0 = time.time()
    spark = get_spark()
    log = {"feeds": generate()}

    sources = contracts.load_sources()
    log["bronze"] = {s["feed"]: bronze.ingest(spark, s) for s in sources}
    # replay every feed: idempotence means nothing changes
    log["bronze_after_replay"] = {s["feed"]: bronze.ingest(spark, s) for s in sources}

    tx_contract = next(s for s in sources if s["feed"] == "platform_transactions")
    valid, quarantined = silver.split_by_contract(spark, tx_contract)
    log["silver"] = {"platform_transaction": valid, "quarantine": quarantined,
                     "quarantine_reasons": {r["rule"]: r["n"] for r in spark.sql(
                         f"SELECT explode(_failed_rules) AS rule, count(*) AS n FROM "
                         f"{table('silver', 'platform_transaction_quarantine')} GROUP BY 1").collect()}}
    silver.build_dim_fund_share_class(spark)
    silver.build_conformed(spark)

    gold.build_fct_platform_transaction(spark)
    first = spark.table(table("gold", "fct_platform_transaction")).count()
    gold.build_fct_platform_transaction(spark)       # incremental re-run inside the lookback window
    log["gold_fact"] = {"rows": first, "rows_after_rerun": spark.table(table("gold", "fct_platform_transaction")).count()}
    for build in gold.PRODUCTS:
        build(spark)

    compiled = TARGET / "compiled"
    compiled.mkdir(parents=True, exist_ok=True)
    log["contracts"] = []
    for c in contracts.load_products():
        (compiled / f"{c['product']}_expectations.py").write_text(contracts.compile_expectations(c))
        (compiled / f"{c['product']}_governance.sql").write_text(contracts.compile_governance(c))
        res = contracts.run_checks(spark, c)
        log["contracts"].append({"product": c["product"], "department": c["department"],
                                 "passed": sum(r["status"] == "pass" for r in res), "total": len(res),
                                 "results": res})
    log["seconds"] = round(time.time() - t0, 1)
    (TARGET / "run_log.json").write_text(json.dumps(log, indent=2, default=str))
    return log


def summarise(out: dict) -> None:
    print(json.dumps({k: v for k, v in out.items() if k != "contracts"}, indent=2, default=str))
    for c in out["contracts"]:
        print(f"{c['department']:<10} {c['product']:<32} {c['passed']}/{c['total']} checks passed")
        for r in c["results"]:
            if r["status"] == "fail":
                print(f"           FAIL {r['check']}: {r['failing_rows']} row(s) {r['sample'][:1]}")


if __name__ == "__main__":
    summarise(run())
