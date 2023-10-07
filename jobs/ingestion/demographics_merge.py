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

from cvmdatalake import staging_to_conformed_full_load_overwrite, staging, conformed

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'lake_descriptor', 'cvm_environment'])

sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
spark = glueContext.spark_session

lake_descriptor = args['lake_descriptor']
environment = args['cvm_environment']

staging_to_conformed_full_load_overwrite(glueContext, stg_table=staging.Demographics, conf_table=conformed.Demographics, on_primary_key='idi_counterparty', drop_duplicates_on_primary=True, lake_descriptor=lake_descriptor)

# conformed_trx_path = get_s3_path(conformed.Demographics, lake_descriptor)
#
# stg_map_path = get_s3_path(staging.Demographics, lake_descriptor)
# stg_demographics_df = DeltaTable.forPath(spark, stg_map_path).toDF()
#
# creat_delta_table_if_not_exists(spark, conformed.Demographics, lake_descriptor)
# conformed_demographics_path = get_s3_path(conformed.Demographics, lake_descriptor)
# conformed_demographics = DeltaTable.forPath(spark, conformed_demographics_path)
#
# conformed_demographics.alias('conformed_demographics').merge(
#     stg_demographics_df.alias('stg_demographics'),
#     """
#     stg_demographics.bu = conformed_demographics.bu AND
#     stg_demographics.idi_counterparty = conformed_demographics.idi_counterparty AND
#     """
# ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
#
# job.commit()
