from jobs.algorithms.rfm.prepare_features_for_transform import rfm_transform


def test_rfm_transform_with_dev_data(spark):
    trx_df = spark.read.parquet('/home/sad/Downloads/transactions.parquet')
    prod_df = spark.read.parquet('/home/sad/Downloads/products.parquet')

    result = rfm_transform(trx_df, prod_df).collect()

    print(result)
