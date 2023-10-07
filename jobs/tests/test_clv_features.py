from cvmdatalake.data_filters import cvm_data_filter_registry
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType, DateType

from cvmdatalake import DataFilterContext
from jobs.algorithms.clv.prepare_features_for_training import clv_training, filter_for_bu_context
from jobs.algorithms.clv.prepare_features_for_transform import clv_transform


def test_clv_transform_with_dev_data(spark):
    trx_df = spark.read.parquet('/home/sad/transactions.parquet')
    prod_df = spark.read.parquet('/home/sad/products.parquet')
    id_map_df = spark.read.parquet('/home/sad/id_map.parquet')

    result = clv_transform(trx_df).collect()

    print(result)

    test = 12


def test_clv_training_with_dev_data(spark):
    trx_df = spark.read.parquet('/home/sad/Downloads/trx_new.parquet')
    trx_df = trx_df.withColumn('dat_date_type_1', lit('2023-01-13').cast(DateType()))

    prod_df = spark.read.parquet('/home/sad/Downloads/products.parquet')

    result = clv_training(trx_df, prod_df)
    result.printSchema()

    print(result.collect())


def test_numberic(spark):
    sample_df = spark.createDataFrame(data=[
        (1, "C1234"),
        (2, "1234"),
        (3, "9478400023090422"),
        (4, "C9478400023090422"),
    ], schema=StructType([
        StructField('id', IntegerType()),
        StructField('test', StringType()),
    ]))


    sample_df = sample_df.withColumn('test_2', col('test').cast(LongType()).isNotNull())
    sample_df.show(10, truncate=False)


def test_filtering(spark):
    sample_df = spark.createDataFrame(data=[
        ("share", "S1"),
        ("share", "S2"),
        ("share", "S3"),
        ("crf", "C1"),
        ("crf", "C2"),
    ], schema=StructType([
        StructField('bu', StringType()),
        StructField('test', StringType()),
    ]))

    clv_data_filter = cvm_data_filter_registry.get_data_filter(DataFilterContext.clv_bu_share)
    a, b = clv_data_filter(trx=sample_df, prod=sample_df)

    a.show(5)
    b.show(5)
