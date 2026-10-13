"""Gold: one data product per department, each built to the contract that department signed."""
from datetime import datetime

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from nexus.silver import write
from nexus.spark import table

FLOW_TYPES = ["contribution", "withdrawal", "fee"]          # Finance's Net Flows definition
LOOKBACK = "INTERVAL 72 HOURS"                               # p99.5 of observed late-arrival delay


def build_fct_platform_transaction(spark: SparkSession) -> None:
    fact_name = table("gold", "fct_platform_transaction")
    if spark.catalog.tableExists(fact_name):
        hwm = spark.table(fact_name).agg(F.max("_ingested_at")).first()[0] or datetime(1900, 1, 1)
    else:
        hwm = datetime(1900, 1, 1)                           # first run = full load

    src = (spark.table(table("silver", "platform_transaction"))
           .filter(F.col("_ingested_at") > F.lit(hwm) - F.expr(LOOKBACK)))
    fund = spark.table(table("silver", "dim_fund_share_class"))
    acct = spark.table(table("silver", "account"))
    client = spark.table(table("silver", "dim_client"))

    # point in time: the share-class version in force on the trade date
    fact = (src.alias("s")
            .join(fund.alias("f"), (F.col("f.fund_nk") == F.col("s.isin"))
                  & (F.col("s.trade_date") >= F.col("f.valid_from"))
                  & (F.col("s.trade_date") < F.col("f.valid_to")), "left")
            .join(acct.alias("a"), "account_id")
            .join(client.alias("c"), "client_id")
            .select(F.xxhash64("s.transaction_id").alias("transaction_sk"), "s.transaction_id",
                    "f.fund_sk", "f.ongoing_charge_pct", "s.account_id", "c.client_sk", "s.wrapper_code",
                    "c.legal_entity", "c.channel", "s.trade_date", "s.transaction_type", "s.amount_gbp",
                    "s._ingested_at"))

    if not spark.catalog.tableExists(fact_name):
        fact.limit(0).write.format("delta").saveAsTable(fact_name)
    (DeltaTable.forName(spark, fact_name).alias("t")
       .merge(fact.alias("u"), "t.transaction_sk = u.transaction_sk")
       .whenMatchedUpdateAll()
       .whenNotMatchedInsertAll()
       .execute())


def finance_net_flows_daily(spark: SparkSession) -> None:
    fct = spark.table(table("gold", "fct_platform_transaction")).filter(F.col("transaction_type").isin(FLOW_TYPES))
    write(fct.groupBy("legal_entity", "wrapper_code", "trade_date").agg(
        F.round(F.sum(F.when(F.col("amount_gbp") > 0, F.col("amount_gbp")).otherwise(0)), 2).alias("gross_inflows_gbp"),
        F.round(-F.sum(F.when(F.col("amount_gbp") < 0, F.col("amount_gbp")).otherwise(0)), 2).alias("gross_outflows_gbp"),
        F.round(F.sum("amount_gbp"), 2).alias("net_flows_gbp")), "gold", "finance_net_flows_daily")


def marketing_acquisition_monthly(spark: SparkSession) -> None:
    first = (spark.table(table("silver", "account")).groupBy("client_id").agg(F.min("opened_on").alias("first_open"))
             .join(spark.table(table("silver", "dim_client")).select("client_id", "channel"), "client_id")
             .withColumn("month", F.trunc("first_open", "month"))
             .filter(F.col("month") >= F.lit("2025-10-01")))
    new = first.groupBy("month", "channel").agg(F.count("*").alias("new_clients"))
    spend = spark.table(table("silver", "marketing_spend"))
    write(spend.join(new, ["month", "channel"], "left").fillna(0, ["new_clients"])
          .withColumn("cost_per_acquisition_gbp",
                      F.round(F.col("spend_gbp") / F.nullif(F.col("new_clients"), F.lit(0)), 2))
          .select("month", "channel", "new_clients", "spend_gbp", "cost_per_acquisition_gbp"),
          "gold", "marketing_acquisition_monthly")


def _months(spark: SparkSession):
    return spark.sql("SELECT explode(sequence(DATE'2025-10-01', DATE'2026-09-01', INTERVAL 1 MONTH)) AS month")


def hr_workforce_monthly(spark: SparkSession, min_cell: int = 5) -> None:
    col = spark.table(table("silver", "colleague"))
    m = (_months(spark).withColumn("month_end", F.last_day("month"))
         .withColumn("window_start", F.add_months(F.col("month_end"), -12)))
    agg = m.crossJoin(col).groupBy("month", "function").agg(
        F.sum(F.when((F.col("hired_on") <= F.col("month_end"))
                     & (F.col("left_on").isNull() | (F.col("left_on") > F.col("month_end"))), 1).otherwise(0)).alias("headcount"),
        F.sum(F.when((F.col("left_on") > F.col("window_start"))
                     & (F.col("left_on") <= F.col("month_end")), 1).otherwise(0)).alias("leavers_12m"))
    # small cells are suppressed, never published: a count of 2 in a small team identifies people
    write(agg.withColumn("leavers_12m", F.when(F.col("leavers_12m") >= min_cell, F.col("leavers_12m")))
          .withColumn("attrition_rate_12m",
                      F.round(F.col("leavers_12m") / (F.col("headcount") + F.col("leavers_12m") / 2), 4))
          .select("month", "function", "headcount", "leavers_12m", "attrition_rate_12m"), "gold", "hr_workforce_monthly")


def executive_kpis_monthly(spark: SparkSession) -> None:
    m = _months(spark).withColumn("month_end", F.last_day("month"))
    acct = spark.table(table("silver", "account"))
    fct = spark.table(table("gold", "fct_platform_transaction"))
    tx = fct.select(F.col("account_id").alias("tx_account"), "trade_date", "amount_gbp")
    flows_to = (m.crossJoin(acct.select("account_id", "client_id", "opened_on", "opening_value"))
                .filter(F.col("opened_on") <= F.col("month_end"))
                .join(tx, (F.col("account_id") == F.col("tx_account"))
                      & (F.col("trade_date") <= F.col("month_end")), "left")
                .groupBy("month", "account_id", "client_id", "opening_value")
                .agg(F.coalesce(F.sum("amount_gbp"), F.lit(0)).alias("cum_flows")))
    aua = flows_to.groupBy("month").agg(
        F.round(F.sum(F.greatest(F.col("opening_value") + F.col("cum_flows"), F.lit(0))), 0).alias("aua_gbp"),
        F.countDistinct("client_id").alias("active_clients"))
    flows = (spark.table(table("gold", "finance_net_flows_daily"))
             .groupBy(F.trunc("trade_date", "month").alias("month"))
             .agg(F.round(F.sum("net_flows_gbp"), 2).alias("net_flows_gbp")))
    write(aua.join(flows, "month", "left").orderBy("month"), "gold", "executive_kpis_monthly")


def product_journeys_monthly(spark: SparkSession) -> None:
    j = (spark.table(table("silver", "service_journey"))
         .withColumn("month", F.trunc("created_on", "month"))
         .withColumn("days", F.datediff("completed_on", "created_on")))
    write(j.groupBy("month", "journey_type").agg(
        F.count("*").alias("requested"),
        F.count("completed_on").alias("completed"),
        F.percentile_approx("days", 0.5).alias("median_days"),
        F.percentile_approx("days", 0.9).alias("p90_days"))
        .withColumn("completion_rate", F.round(F.col("completed") / F.col("requested"), 4)),
        "gold", "product_journeys_monthly")


PRODUCTS = [finance_net_flows_daily, marketing_acquisition_monthly, hr_workforce_monthly,
            executive_kpis_monthly, product_journeys_monthly]
