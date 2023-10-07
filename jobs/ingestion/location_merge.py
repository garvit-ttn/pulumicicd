from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from delta.tables import *
from pyspark.sql.functions import *

from cvmdatalake import staging_to_conformed_full_load, staging, conformed

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'lake_descriptor', 'cvm_environment'])

sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
spark = glueContext.spark_session

lake_descriptor = args['lake_descriptor']
environment = args['cvm_environment']

staging_to_conformed_full_load(glueContext, stg_table=staging.Location, conf_table=conformed.Location, on_primary_key='cde_store_key', lake_descriptor=lake_descriptor)

# stg_promo_path = get_s3_path(staging.Location, lake_descriptor)
# stg_Location_df = DeltaTable.forPath(spark, stg_promo_path).toDF()
#
# creat_delta_table_if_not_exists(spark, conformed.Location, lake_descriptor)
# conformed_Location_path = get_s3_path(conformed.Location, lake_descriptor)
# conformed_Location = DeltaTable.forPath(spark, conformed_Location_path)
#
# conformed_Location.alias(conformed.Location.table_alias()).merge(
#     stg_Location_df.alias(staging.Location.table_alias()),
#     f"""
#     {staging.Location.cde_store_code.column_alias()} = {conformed.Location.cde_store_code.column_alias()}
#     """
# ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
