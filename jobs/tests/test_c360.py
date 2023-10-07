import json

from delta.tables import DeltaTable
from pandas import DataFrame
from pyspark.sql.functions import trim, substring, col

import jobs.ingestion.c360 as c360_new
from cvmdatalake import creat_delta_table_if_not_exists, conformed, features


def test_c360_builder(spark):
    # desc = {conformed.CustomerProfile.table_id(): '/home/sad/test-profiles'}
    # creat_delta_table_if_not_exists(spark, conformed.CustomerProfile, json.dumps(desc))

    demo = spark.read.parquet('/home/sad/Downloads/demographics.parquet')
    id_map = spark.read.parquet('/home/sad/Downloads/id_map.parquet')
    braze = spark.read.parquet('/home/sad/Downloads/custom_aattribs.parquet')

    clv = c360_new.parse_clv_output(spark, '/home/sad/Downloads/clv.csv.out')

    nbo = c360_new.parse_nbo_output(spark, '/home/sad/Downloads/nbo.csv.out')

    rfm = spark.read.csv(
        '/home/sad/Downloads/output.csv',
        header=True
    )

    kpis = spark.read.parquet(
        '/home/sad/Downloads/kpi.parquet',
    )

    result = c360_new.run(demo, id_map, braze, rfm, clv, kpis, nbo)

    result.printSchema()


def test_nbo_parse(spark):
    result = c360_new.parse_nbo_output(spark, '/home/sad/Downloads/nbo.csv.out')

    result.show(10, truncate=False)
