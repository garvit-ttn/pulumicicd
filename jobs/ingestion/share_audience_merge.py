import sys

from awsglue import DynamicFrame
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from delta import DeltaTable
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import *

from cvmdatalake import creat_delta_table_if_not_exists, create_delta_table_from_catalog, get_s3_path, staging, conformed

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'lake_descriptor', 'cvm_environment'])

sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
spark = glueContext.spark_session

lake_descriptor = args['lake_descriptor']
environment = args['cvm_environment']

creat_delta_table_if_not_exists(glueContext.spark_session, conformed.ShareAudience, lake_descriptor)
stg_dataset = create_delta_table_from_catalog(glueContext.spark_session, staging.ShareAudience, lake_descriptor).toDF()
stg_dataset.write.format("delta").mode("overwrite").save(get_s3_path(conformed.ShareAudience, lake_descriptor))
