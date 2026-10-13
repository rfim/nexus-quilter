"""Data contracts: the YAML a department signs, compiled into tests, expectations and grants.

One file in, three artefacts out:
  1. checks      -> SQL run against the gold product on every load (results are kept)
  2. expectations-> a Lakeflow / Spark Declarative Pipelines expectation set
  3. governance  -> Unity Catalog SQL: owner, tags, grants
"""
from pathlib import Path

import yaml
from pyspark.sql import SparkSession

from nexus.spark import ROOT, table

CONTRACTS = ROOT / "contracts"


def load_products() -> list[dict]:
    out = []
    for p in sorted((CONTRACTS / "products").glob("*.yml")):
        c = yaml.safe_load(p.read_text())
        c["_path"] = str(p.relative_to(ROOT))
        out.append(c)
    return out


def load_sources() -> list[dict]:
    return yaml.safe_load((CONTRACTS / "sources" / "sources.yml").read_text())


def _q(v) -> str:
    return "'" + str(v).replace("'", "''") + "'"


def compile_checks(c: dict) -> list[dict]:
    """Each check is a SQL query that returns the failing rows. Zero rows = pass."""
    t = table("gold", c["product"])
    checks = []
    for col in c.get("columns", []):
        for test in col.get("tests", []):
            name, arg = (test, None) if isinstance(test, str) else next(iter(test.items()))
            if name == "not_null":
                sql = f"SELECT * FROM {t} WHERE {col['name']} IS NULL"
            elif name == "accepted_values":
                sql = f"SELECT * FROM {t} WHERE {col['name']} NOT IN ({', '.join(_q(v) for v in arg)})"
            elif name == "range":
                cond = [f"{col['name']} < {arg['min']}"] if "min" in arg else []
                cond += [f"{col['name']} > {arg['max']}"] if "max" in arg else []
                sql = f"SELECT * FROM {t} WHERE {' OR '.join(cond)}"
            else:
                raise ValueError(f"unknown test {name} in {c['_path']}")
            checks.append({"check": f"{name}: {col['name']}", "kind": "column", "sql": sql})

    g = ", ".join(c["grain"])
    checks.append({"check": f"unique grain: {g}", "kind": "grain",
                   "sql": f"SELECT {g}, count(*) AS n FROM {t} GROUP BY {g} HAVING count(*) > 1"})

    f = c["freshness"]
    checks.append({"check": f"freshness: {f['column']} within {f['max_lag_days']} day(s) of the run",
                   "kind": "freshness",
                   "sql": f"SELECT max({f['column']}) AS latest FROM {t} "
                          f"HAVING max({f['column']}) < DATE'2026-09-30' - INTERVAL {f['max_lag_days']} DAYS"})

    if r := c.get("reconcile"):
        on = ", ".join(r["match_on"])
        checks.append({"check": f"reconcile to {r['against']} within £{r['tolerance_gbp']}", "kind": "reconcile",
                       "sql": f"WITH p AS (SELECT {on}, round(sum({r['measure']}), 2) AS platform FROM {t} GROUP BY {on}) "
                              f"SELECT {on}, platform, l.{r['ledger_measure']} AS ledger, "
                              f"round(platform - l.{r['ledger_measure']}, 2) AS difference "
                              f"FROM p FULL JOIN {table('silver', r['against'])} l USING ({on}) "
                              f"WHERE abs(coalesce(platform, 0) - coalesce(l.{r['ledger_measure']}, 0)) > {r['tolerance_gbp']}"})

    if r := c.get("reconcile_to_product"):
        other = table("gold", r["product"])
        checks.append({"check": f"same number as {r['product']}", "kind": "reconcile",
                       "sql": f"WITH f AS (SELECT trunc(trade_date, 'MM') AS month, round(sum({r['measure']}), 2) AS finance "
                              f"FROM {other} GROUP BY 1) SELECT e.month, e.{r['measure']}, f.finance FROM {t} e "
                              f"JOIN f USING (month) WHERE abs(e.{r['measure']} - f.finance) > {r['tolerance_gbp']}"})

    if p := c.get("privacy"):
        if m := p.get("min_cell_size"):
            checks.append({"check": f"no published {m['column']} below {m['size']}", "kind": "privacy",
                           "sql": f"SELECT * FROM {t} WHERE {m['column']} < {m['size']}"})
        if p.get("no_client_level_columns") or p.get("no_colleague_level_columns"):
            checks.append({"check": "no person-level columns in the product", "kind": "privacy_columns",
                           "forbidden": ["client_id", "email", "national_insurance_no", "colleague_id", "full_name"],
                           "sql": f"DESCRIBE TABLE {t}  -- fails if any person-level column is present"})
    return checks


def compile_expectations(c: dict) -> str:
    """The same rules as a Lakeflow (Spark Declarative Pipelines) expectation set."""
    rules = {}
    for col in c.get("columns", []):
        for test in col.get("tests", []):
            name, arg = (test, None) if isinstance(test, str) else next(iter(test.items()))
            key = f"{col['name']}_{name}"
            if name == "not_null":
                rules[key] = f"{col['name']} IS NOT NULL"
            elif name == "accepted_values":
                rules[key] = f"{col['name']} IN ({', '.join(_q(v) for v in arg)})"
            elif name == "range":
                parts = ([f"{col['name']} >= {arg['min']}"] if "min" in arg else []) + \
                        ([f"{col['name']} <= {arg['max']}"] if "max" in arg else [])
                rules[key] = " AND ".join(parts)
    body = "\n".join(f'    "{k}": "{v}",' for k, v in rules.items())
    return (f"from pyspark import pipelines as dp\n\n"
            f"# generated from {c['_path']}: do not edit by hand\n"
            f"RULES = {{\n{body}\n}}\n\n"
            f"@dp.materialized_view(name=\"{c['product']}\", comment={c['definition'].strip()!r})\n"
            f"@dp.expect_all_or_fail(RULES)\n"
            f"def {c['product']}():\n"
            f"    return build_{c['product']}()\n")


def compile_governance(c: dict, catalog: str = "nexus") -> str:
    t = f"{catalog}.gold.{c['product']}"
    lines = [f"-- generated from {c['_path']}: do not edit by hand",
             f"ALTER TABLE {t} SET OWNER TO `{c['owner']}`;",
             f"COMMENT ON TABLE {t} IS {_q(c['definition'].strip())};",
             f"ALTER TABLE {t} SET TAGS ('department' = {_q(c['department'])}, "
             f"'grain' = {_q(', '.join(c['grain']))});"]
    lines += [f"GRANT SELECT ON TABLE {t} TO `{g}`;" for g in c["access"]["read"]]
    return "\n".join(lines) + "\n"


def run_checks(spark: SparkSession, c: dict) -> list[dict]:
    results = []
    for chk in compile_checks(c):
        if chk["kind"] == "privacy_columns":
            present = [x for x in spark.table(table("gold", c["product"])).columns if x in chk["forbidden"]]
            n, sample = len(present), [{"column": x} for x in present]
        else:
            failing = spark.sql(chk["sql"])
            n, sample = failing.count(), [r.asDict() for r in failing.limit(3).collect()]
        results.append(chk | {"failing_rows": n, "status": "pass" if n == 0 else "fail", "sample": sample})
    return results
