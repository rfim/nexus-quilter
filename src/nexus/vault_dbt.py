"""Run the dbt Data Vault inside the pipeline's Spark session.

Day 1 loads from scratch, day 1 again must change nothing (insert-only, idempotent),
then day 2 lands the next morning's changes. Returns row counts per table and step.
"""
import shutil
from pathlib import Path

from dbt.cli.main import dbtRunner

from nexus.spark import get_spark

DBT_DIR = Path(__file__).resolve().parents[2] / "dbt"
TABLES = ["hub_client", "hub_adviser", "hub_firm", "hub_portfolio", "hub_journey",
          "link_client_adviser", "link_client_portfolio", "link_firm_adviser", "link_client_journey",
          "sat_client_platform", "sat_client_advice", "sat_client_cheviot", "sat_firm",
          "sat_client_flows", "sat_journey_status", "sat_client_vulnerability", "eff_firm_adviser",
          "dim_client_360", "fct_firm_flows", "journey_outcomes"]


def _dbt(*args):
    res = dbtRunner().invoke([*args, "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR),
                              "--target", "local", "--quiet"])
    if not res.success:
        raise RuntimeError(f"dbt {' '.join(args)} failed: {res.exception or res.result}")


def counts(spark):
    return {t: spark.table(f"nexus_vault.{t}").count() for t in TABLES}


def _local_catalog(spark):
    """Local catalogs are in-memory: re-register the pipeline's Delta tables when run in a fresh process."""
    wh = Path(spark.conf.get("spark.sql.warehouse.dir").removeprefix("file:"))
    shutil.rmtree(wh / "nexus_vault.db", ignore_errors=True)  # day 1 is a full refresh anyway
    for db in ("nexus_silver", "nexus_gold"):
        for t in sorted((wh / f"{db}.db").glob("*/_delta_log")):
            spark.sql(f"CREATE TABLE IF NOT EXISTS {db}.{t.parent.name} USING DELTA LOCATION '{t.parent}'")


def run():
    spark = get_spark()
    _local_catalog(spark)
    spark.sql("CREATE DATABASE IF NOT EXISTS nexus_vault")
    _dbt("seed", "--full-refresh")
    _dbt("build", "--full-refresh", "--vars", "{load_day: 1}")
    day1 = counts(spark)
    _dbt("build", "--vars", "{load_day: 1}")
    replay = counts(spark)
    if replay != day1:
        raise AssertionError(f"replaying day 1 changed the vault: {day1} -> {replay}")
    _dbt("build", "--vars", "{load_day: 2}")
    day2 = counts(spark)
    v = "nexus_vault"
    return {
        "engine": "dbt-core + dbt-spark (session) on Delta; dbt-databricks in production",
        "day1": day1, "day2": day2, "replay_unchanged": True,
        "firm_moves": spark.sql(f"select count(*) from {v}.eff_firm_adviser where not is_current").first()[0],
        "fct_firm_flows": [r.asDict() for r in spark.sql(f"select * from {v}.fct_firm_flows order by firm_name").collect()],
        "journey_outcomes": [r.asDict() for r in spark.sql(f"select * from {v}.journey_outcomes order by 1, 2").collect()],
    }


if __name__ == "__main__":
    import json
    print(json.dumps(run(), indent=1, default=str))
