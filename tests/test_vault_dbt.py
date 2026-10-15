"""The dbt Data Vault parses and wires up: every model resolves its refs and every test attaches."""
from pathlib import Path

from dbt.cli.main import dbtRunner

DBT = Path(__file__).resolve().parents[1] / "dbt"


def test_vault_project_parses():
    res = dbtRunner().invoke(["parse", "--project-dir", str(DBT), "--profiles-dir", str(DBT), "--quiet"])
    assert res.success, res.exception
    nodes = res.result.nodes.values()
    models = {n.name for n in nodes if n.resource_type == "model"}
    assert {"hub_client", "link_firm_adviser", "sat_journey_status", "eff_firm_adviser", "journey_outcomes"} <= models
    assert sum(n.resource_type == "test" for n in nodes) >= 25
