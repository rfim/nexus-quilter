"""Bronze: land every feed as Delta, written by MERGE so a replayed batch changes nothing."""
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from nexus.spark import ROOT, table

LANDING = ROOT / "landing"


def upsert_bronze(spark: SparkSession, micro_batch: DataFrame, cfg: dict) -> None:
    keys, order, target = cfg["merge_keys"], cfg.get("ordering_key"), cfg["table"]
    latest = micro_batch
    if order:
        # deduplicate INSIDE the batch, so a replayed offset range cannot be applied twice
        w = Window.partitionBy(*keys).orderBy(F.col(order).desc())
        latest = micro_batch.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
    else:
        latest = micro_batch.dropDuplicates(keys)

    if not spark.catalog.tableExists(target):
        latest.limit(0).write.format("delta").saveAsTable(target)

    cond = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    merge = DeltaTable.forName(spark, target).alias("t").merge(latest.alias("s"), cond)
    if order:
        merge = merge.whenMatchedUpdateAll(condition=f"s.{order} > t.{order}")
    merge.whenNotMatchedInsertAll().execute()


def ingest(spark: SparkSession, contract: dict) -> int:
    """Batch read of one landing feed; on Databricks the same function sits behind Auto Loader."""
    src, tgt = contract["source"], contract["target"]
    df = (spark.read.json(str(LANDING / src["feed"]))
          .withColumn("_ingested_at", F.current_timestamp())
          .withColumn("_source_file", F.col("_metadata.file_path")))
    upsert_bronze(spark, df, {"table": table("bronze", tgt["table"]), "merge_keys": tgt["merge_keys"],
                              "ordering_key": tgt.get("ordering_key")})
    return spark.table(table("bronze", tgt["table"])).count()
