from nexus.bronze import upsert_bronze
from nexus.spark import table

CFG = {"table": table("bronze", "t_idempotence"), "merge_keys": ["id"], "ordering_key": "lsn"}


def test_replay_is_a_no_op_and_higher_lsn_wins(spark):
    spark.sql(f"DROP TABLE IF EXISTS {CFG['table']}")
    batch = spark.createDataFrame([(1, 10, 100.0), (2, 20, 50.0), (1, 10, 100.0)], "id INT, lsn INT, amount DOUBLE")
    upsert_bronze(spark, batch, CFG)
    upsert_bronze(spark, batch, CFG)                       # replayed offsets
    assert spark.table(CFG["table"]).count() == 2

    correction = spark.createDataFrame([(1, 11, 101.0), (2, 19, 0.0)], "id INT, lsn INT, amount DOUBLE")
    upsert_bronze(spark, correction, CFG)
    got = {r.id: r.amount for r in spark.table(CFG["table"]).collect()}
    assert got == {1: 101.0, 2: 50.0}                      # stale lsn 19 never overwrites lsn 20
