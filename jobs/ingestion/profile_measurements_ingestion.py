import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from awsglue import DynamicFrame
from awsglue.transforms import *
from delta import DeltaTable
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.functions import col
import pyspark.sql.functions as func
from pyspark.sql.window import Window
from pyspark.sql.types import *
import boto3
from botocore.exceptions import ClientError
import pandas as pd
import time
from delta.tables import *
from pyspark.sql.types import DateType

from cvmdatalake import creat_delta_table_if_not_exists, staging, conformed, get_s3_path, upsert_to_deltatable

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'lake_descriptor', 'cvm_environment'])

sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
spark = glueContext.spark_session

lake_descriptor = args['lake_descriptor']
environment = args['cvm_environment']

creat_delta_table_if_not_exists(spark, conformed.ProfileMeasurements, lake_descriptor)
measurements_path = get_s3_path(conformed.ProfileMeasurements, lake_descriptor)
measurements = DeltaTable.forPath(spark, measurements_path).toDF()



df = spark.read.format("csv").option("header", "true").load("s3://cvm-prod-conformed-dd6241f/features/measurement/treatment-control/")

# Display the DataFrame
df.show()

df = df.withColumn("run_date", col("run_date").cast(DateType()))

appended_df = measurements.union(df)


appended_df.write \
    .format('delta') \
    .mode('overwrite') \
    .save(measurements_path)





