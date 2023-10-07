from awsglue import DynamicFrame
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import *

from cvmdatalake import landing_to_staging_full_load, landing, staging

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'lake_descriptor', 'cvm_environment'])

sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
spark = glueContext.spark_session

lake_descriptor = args['lake_descriptor']
environment = args['cvm_environment']


landing_to_staging_full_load(glueContext, lnd_table=landing.Location, stg_table=staging.Location, on_primary_key='cde_store_key', prefix=environment, lake_descriptor=lake_descriptor)

# lnd_Location: DynamicFrame = glueContext.create_dynamic_frame.from_catalog(
#     database=landing.Location.catalog_db_name(environment),
#     table_name=landing.Location.catalog_table_name(environment),
#     transformation_ctx=landing.Location.table_id()
# )
#
# lnd_Location_df = lnd_Location.toDF()
#
# lnd_Location_df.printSchema()
#
# lnd_Location_df = lnd_Location_df.drop("ingestion_date").dropDuplicates(
#     [
#         "cde_store_code"
#     ])
#
# creat_delta_table_if_not_exists(spark, staging.Location, lake_descriptor)
# stg_promo_path = get_s3_path(staging.Location, lake_descriptor)
# lnd_Location_df.write \
#     .format('delta') \
#     .mode('overwrite') \
#     .save(stg_promo_path)
