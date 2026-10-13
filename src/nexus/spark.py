"""One SparkSession for local runs and CI. On Databricks the active session is reused."""
import os
from pathlib import Path

from pyspark.sql import SparkSession

ROOT = Path(__file__).resolve().parents[2]
TARGET = ROOT / "target"


def get_spark(app: str = "nexus") -> SparkSession:
    active = SparkSession.getActiveSession()
    if active is not None:  # Databricks: Unity Catalog and Delta are already configured
        return active
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.master("local[2]")
        .appName(app)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", os.environ.get("NEXUS_WAREHOUSE", str(TARGET / "warehouse")))
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "Europe/London")
        .config("spark.ui.enabled", "false")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    for layer in ("bronze", "silver", "gold", "governance"):
        spark.sql(f"CREATE DATABASE IF NOT EXISTS nexus_{layer}")
    return spark


def table(layer: str, name: str) -> str:
    """nexus_<layer>.<name> locally; nexus.<layer>.<name> under Unity Catalog (NEXUS_CATALOG=nexus)."""
    catalog = os.environ.get("NEXUS_CATALOG")
    return f"{catalog}.{layer}.{name}" if catalog else f"nexus_{layer}.{name}"
