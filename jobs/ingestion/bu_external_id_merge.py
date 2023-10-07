
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import *
from cvmdatalake import staging, conformed, create_delta_table_from_catalog

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'lake_descriptor', 'cvm_environment'])

sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
spark = glueContext.spark_session
lake_descriptor = args['lake_descriptor']
environment = args['cvm_environment']
conformed_bu_external_id = create_delta_table_from_catalog(spark, conformed.BuExternalId, lake_descriptor)

staging_bu_external_id_df = create_delta_table_from_catalog(spark, staging.BuExternalId, lake_descriptor).toDF()
print(f"There are {staging_bu_external_id_df.count()} bu_external_ids on staging")
staging_bu_external_id_df.show(truncate=False)

staging_bu_external_id_df: DataFrame = staging_bu_external_id_df.dropDuplicates(
    [
        staging.BuExternalId.gcr_id.name,
        staging.BuExternalId.external_id.name,
        staging.BuExternalId.org_code.name
    ]
)

print(f"There are {staging_bu_external_id_df.count()} bu_external_ids from staging to be merged")

conformed_bu_external_id.alias(conformed.BuExternalId.table_alias()).merge(
    staging_bu_external_id_df.alias(staging.BuExternalId.table_alias()),
    f"""
    {staging.BuExternalId.gcr_id.column_alias()} = {conformed.BuExternalId.gcr_id.column_alias()} and 
    {staging.BuExternalId.external_id.column_alias()} = {conformed.BuExternalId.external_id.column_alias()} and 
    {staging.BuExternalId.org_code.column_alias()} = {conformed.BuExternalId.org_code.column_alias()}
    """
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
