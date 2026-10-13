"""Turn a pipeline run into the site's data: gold products, contract results and the real source code."""
import ast
import json
from datetime import date, datetime
from decimal import Decimal

from nexus import contracts
from nexus.spark import ROOT, TARGET, get_spark, table

SITE_DATA = ROOT / "assets" / "data" / "site.json"
SNIPPETS = {
    "ingest": ("src/nexus/bronze.py", "upsert_bronze"),
    "quality": ("src/nexus/silver.py", "split_by_contract"),
    "scd2": ("src/nexus/silver.py", "build_dim_fund_share_class"),
    "fact": ("src/nexus/gold.py", "build_fct_platform_transaction"),
    "product": ("src/nexus/gold.py", "finance_net_flows_daily"),
    "compile": ("src/nexus/contracts.py", "compile_checks"),
}


def _plain(v):
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, (date, datetime)):
        return v.isoformat()
    return v


def _rows(spark, name: str, order: str) -> list[dict]:
    return [{k: _plain(v) for k, v in r.asDict().items()} for r in spark.table(table("gold", name)).orderBy(order).collect()]


def _function_source(path: str, name: str) -> str:
    src = (ROOT / path).read_text()
    node = next(n for n in ast.walk(ast.parse(src)) if isinstance(n, ast.FunctionDef) and n.name == name)
    return "\n".join(src.splitlines()[node.lineno - 1 - len(node.decorator_list):node.end_lineno])


def export() -> None:
    spark = get_spark()
    log = json.loads((TARGET / "run_log.json").read_text())
    products = {c["product"]: c for c in contracts.load_products()}
    data = {
        "run": {k: log[k] for k in ("feeds", "bronze", "bronze_after_replay", "silver", "gold_fact", "seconds")},
        "products": {
            "finance_net_flows_monthly": [{k: _plain(x) for k, x in r.asDict().items()} for r in spark.sql(
                f"SELECT trunc(trade_date, 'MM') AS month, wrapper_code, round(sum(gross_inflows_gbp), 2) AS inflows, "
                f"round(sum(gross_outflows_gbp), 2) AS outflows, round(sum(net_flows_gbp), 2) AS net_flows "
                f"FROM {table('gold', 'finance_net_flows_daily')} GROUP BY 1, 2 ORDER BY 1, 2").collect()],
            "marketing_acquisition_monthly": _rows(spark, "marketing_acquisition_monthly", "month"),
            "hr_workforce_monthly": _rows(spark, "hr_workforce_monthly", "month"),
            "executive_kpis_monthly": _rows(spark, "executive_kpis_monthly", "month"),
            "product_journeys_monthly": _rows(spark, "product_journeys_monthly", "month"),
        },
        "contracts": [],
        "code": {k: {"path": p, "function": f, "source": _function_source(p, f)} for k, (p, f) in SNIPPETS.items()},
    }
    for c in log["contracts"]:
        spec = products[c["product"]]
        data["contracts"].append({
            "product": c["product"], "department": c["department"], "owner": spec["owner"],
            "question": spec["question"], "path": spec["_path"],
            "yaml": (ROOT / spec["_path"]).read_text(),
            "expectations": (TARGET / "compiled" / f"{c['product']}_expectations.py").read_text(),
            "governance": (TARGET / "compiled" / f"{c['product']}_governance.sql").read_text(),
            "passed": c["passed"], "total": c["total"],
            "results": [{k: r[k] for k in ("check", "kind", "status", "failing_rows", "sql", "sample")} for r in c["results"]],
        })
    SITE_DATA.parent.mkdir(parents=True, exist_ok=True)
    SITE_DATA.write_text(json.dumps(data, default=_plain, separators=(",", ":")))


if __name__ == "__main__":
    export()
    print(f"wrote {SITE_DATA.relative_to(ROOT)} ({SITE_DATA.stat().st_size // 1024} KB)")
