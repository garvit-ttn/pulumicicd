import os

from delta import DeltaTable


def test_delta_history(spark):
    table_path = "/home/sad/Downloads/sample_delta_table"
    os.system(f"rm -rf {table_path}")

    DeltaTable.create(spark) \
        .location(table_path) \
        .addColumn("braze_id", "string") \
        .addColumn("audience", "string") \
        .property("delta.enableChangeDataFeed", "true") \
        .execute()

    df = spark.createDataFrame(
        [
            ("1", "A"),
            ("2", "B")
        ],
        ["braze_id", "audience"],
    )

    df.write.format('delta').mode('overwrite').save(table_path)

    df = spark.createDataFrame(
        [
            ("1", "A"),
            ("2", "E"),
            ("2", "C"),
        ],
        ["braze_id", "audience"],
    )

    dst_table = DeltaTable.forPath(spark, table_path)
    dst_table.alias('dst').merge(
        df.alias('src'),
        "dst.braze_id = src.braze_id and dst.audience = src.audience"
    ).whenNotMatchedBySourceDelete().execute()  # requires spark 3.4.0, not supported by Glue

    DeltaTable.forPath(spark, table_path).toDF().show()

    cdf = spark.read.format("delta") \
        .option("readChangeFeed", "true") \
        .option("startingVersion", 1) \
        .option("endingVersion", 2) \
        .load(table_path)

    cdf.orderBy('_commit_version').show()
