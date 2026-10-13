from pyspark.sql import functions as F

from nexus.spark import table


def test_fund_versions_never_overlap(spark):
    spark.sql(f"CREATE OR REPLACE TABLE {table('bronze', 'fund')} USING DELTA AS "
              "SELECT 'GB00QAAAAAA1' AS isin, 'Test' AS fund_name, 'Equity' AS asset_class")
    spark.sql(f"CREATE OR REPLACE TABLE {table('bronze', 'fund_charge')} USING DELTA AS SELECT * FROM VALUES "
              "('GB00QAAAAAA1', 0.50, 4, '2025-01-01'), ('GB00QAAAAAA1', 0.42, 4, '2026-04-01') "
              "AS t(isin, ongoing_charge_pct, risk_rating, effective_from)")
    from nexus.silver import build_dim_fund_share_class
    build_dim_fund_share_class(spark)
    dim = spark.table(table("silver", "dim_fund_share_class")).orderBy("valid_from").collect()
    assert [float(r.ongoing_charge_pct) for r in dim] == [0.50, 0.42]
    assert dim[0].valid_to == dim[1].valid_from and dim[1].is_current and not dim[0].is_current
