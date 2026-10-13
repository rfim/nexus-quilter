from nexus import contracts


def test_every_department_has_a_contract_with_an_owner():
    products = contracts.load_products()
    assert {c["department"] for c in products} == {"Finance", "Marketing", "HR", "Executive", "Product"}
    assert all(c["owner"] and c["access"]["read"] for c in products)


def test_contract_compiles_to_checks_expectations_and_grants():
    finance = next(c for c in contracts.load_products() if c["department"] == "Finance")
    kinds = {chk["kind"] for chk in contracts.compile_checks(finance)}
    assert {"column", "grain", "freshness", "reconcile"} <= kinds
    assert "@dp.expect_all_or_fail(RULES)" in contracts.compile_expectations(finance)
    assert "GRANT SELECT ON TABLE nexus.gold.finance_net_flows_daily TO `grp_finance`;" in \
        contracts.compile_governance(finance)


def test_hr_contract_never_publishes_people():
    hr = next(c for c in contracts.load_products() if c["department"] == "HR")
    kinds = [chk["kind"] for chk in contracts.compile_checks(hr)]
    assert "privacy" in kinds and "privacy_columns" in kinds
