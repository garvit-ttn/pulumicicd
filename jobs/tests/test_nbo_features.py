from jobs.algorithms.nbo.prepare_features_for_transform import nbo_transform
from jobs.algorithms.nbo.prepare_features_for_training import nbo_training


def test_nbo_transform_with_dev_data(spark):
    trx_df = spark.read.parquet('/home/sad/Downloads/transactions.parquet')
    prod_df = spark.read.parquet('/home/sad/Downloads/products.parquet')
    # id_map_df = spark.read.parquet('/home/sad/Downloads/id_map.parquet')

    result = nbo_transform(trx_df, prod_df)
    result.printSchema()

    print(result.collect())


def test_nbo_training_with_dev_data(spark):
    trx_df = spark.read.parquet('/home/sad/Downloads/transactions.parquet')
    prod_df = spark.read.parquet('/home/sad/Downloads/products.parquet')
    # id_map_df = spark.read.parquet('/home/sad/Downloads/id_map.parquet')

    result = nbo_training(trx_df, prod_df)
    result.printSchema()

    print(result.collect())
