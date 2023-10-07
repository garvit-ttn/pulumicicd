import os

import pytest
from pyspark.sql import SparkSession

os.environ['AWS_SECRET_ACCESS_KEY'] = ''
os.environ['AWS_ACCESS_KEY_ID'] = ''


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder.master("local[4]")  # specifies that spark is running on a local machine with one thread
        .appName("local-tests")
        .config("spark.executor.cores", "1")
        .config("spark.executor.instances", "1")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.jars.packages", "org.apache.spark:spark-hadoop-cloud_2.12:3.2.0")
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.2.0")
        .getOrCreate()
    )

    yield spark
    spark.stop()
