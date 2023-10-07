from awsglue import DynamicFrame
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from cvmdatalake import landing, create_dynamic_frame_from_catalog
from delta.tables import *
from pyspark.sql.functions import *

from cvmdatalake import landing_to_staging_full_load_overwrite, landing, staging

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'lake_descriptor', 'cvm_environment'])

sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
spark = glueContext.spark_session

lake_descriptor = args['lake_descriptor']
environment = args['cvm_environment']

landing_to_staging_full_load_overwrite(glueContext, lnd_table=landing.Demographics, stg_table=staging.Demographics, on_primary_key='idi_counterparty', drop_duplicates_on_primary=True, prefix=environment, lake_descriptor=lake_descriptor)

# lnd_demographics: DynamicFrame = glueContext.create_dynamic_frame.from_catalog(
#     database=landing.Demographics.catalog_db_name(environment),
#     table_name=landing.Demographics.catalog_table_name(environment),
#     transformation_ctx=landing.Demographics.table_id()
# )
#
# lnd_demographics_df = lnd_demographics.toDF().dropDuplicates(
#     [
#         landing.Demographics.bu.name,
#         landing.Demographics.idi_counterparty.name
#     ])
#
# creat_delta_table_if_not_exists(spark, staging.Demographics, lake_descriptor)
#
# stg_map_path = get_s3_path(staging.Demographics, lake_descriptor)
#
# stg_demographics = DeltaTable.forPath(spark, stg_map_path)
# stg_demographics.alias('stg_demographics').merge(
#     lnd_demographics_df.alias('lnd_demographics'),
#     f"""
#     stg_demographics.{staging.Demographics.bu.name} = lnd_demographics.{landing.Demographics.bu.name} AND
#     stg_demographics.{staging.Demographics.idi_counterparty.name} = lnd_demographics.{landing.Demographics.idi_counterparty.name}
#     """
# ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
