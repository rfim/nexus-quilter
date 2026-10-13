"""Silver: typed, deduplicated, conformed. Quality rules come from the source contract."""
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from nexus.spark import table


def write(df, layer: str, name: str) -> None:
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table(layer, name))


def split_by_contract(spark: SparkSession, contract: dict) -> tuple[int, int]:
    """Clean rows continue; failing rows go to quarantine with the failed rule names attached."""
    rules = contract["quality"]["rules"]
    raw = spark.table(table("bronze", contract["target"]["table"])).withColumn("trade_date", F.to_date("trade_date"))
    passed = " AND ".join(f"({e})" for e in rules.values())
    valid = raw.filter(passed).filter("op <> 'D'")
    quarantine = (raw.filter(f"NOT ({passed})")
                  .withColumn("_failed_rules", F.array_compact(F.array(*[
                      F.when(~F.expr(e), F.lit(n)) for n, e in rules.items()])))
                  .withColumn("_quarantined_at", F.current_timestamp()))
    write(valid, "silver", "platform_transaction")
    write(quarantine, "silver", "platform_transaction_quarantine")
    return valid.count(), quarantine.count()


def build_dim_fund_share_class(spark: SparkSession) -> None:
    """SCD2 from the charge history: a repriced share class never rewrites a past period."""
    charges = spark.table(table("bronze", "fund_charge")).withColumn("effective_from", F.to_date("effective_from"))
    w = Window.partitionBy("isin").orderBy("effective_from")
    dim = (charges.join(spark.table(table("bronze", "fund")), "isin")
           .withColumn("valid_from", F.col("effective_from"))
           .withColumn("valid_to", F.coalesce(F.lead("effective_from").over(w), F.lit("9999-12-31").cast("date")))
           .withColumn("is_current", F.col("valid_to") == F.lit("9999-12-31").cast("date"))
           .withColumn("fund_sk", F.xxhash64("isin", "valid_from"))
           .select("fund_sk", F.col("isin").alias("fund_nk"), "fund_name", "asset_class",
                   "ongoing_charge_pct", "risk_rating", "valid_from", "valid_to", "is_current"))
    write(dim, "silver", "dim_fund_share_class")


def build_conformed(spark: SparkSession) -> None:
    write(spark.table(table("bronze", "client")).withColumn("joined_on", F.to_date("joined_on"))
          .withColumn("client_sk", F.xxhash64("client_id")), "silver", "dim_client")
    write(spark.table(table("bronze", "account"))
          .withColumn("opened_on", F.to_date("opened_on")), "silver", "account")
    write(spark.table(table("bronze", "service_journey"))
          .withColumn("created_on", F.to_date("created_on"))
          .withColumn("completed_on", F.to_date("completed_on")), "silver", "service_journey")
    write(spark.table(table("bronze", "colleague"))
          .withColumn("hired_on", F.to_date("hired_on"))
          .withColumn("left_on", F.to_date("left_on")), "silver", "colleague")
    write(spark.table(table("bronze", "finance_ledger"))
          .withColumn("trade_date", F.to_date("trade_date")), "silver", "finance_ledger")
    write(spark.table(table("bronze", "marketing_spend"))
          .withColumn("month", F.to_date("month")), "silver", "marketing_spend")
