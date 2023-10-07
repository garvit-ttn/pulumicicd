from awsglue import DynamicFrame
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import *
from delta.tables import *

from cvmdatalake import creat_delta_table_if_not_exists, landing, staging, get_s3_path

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'lake_descriptor', 'cvm_environment'])

sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
spark = glueContext.spark_session

lake_descriptor = args['lake_descriptor']
environment = args['cvm_environment']

lnd_id_mapping: DynamicFrame = glueContext.create_dynamic_frame.from_catalog(
    database=landing.IdMapping.catalog_db_name(environment),
    table_name=landing.IdMapping.catalog_table_name(environment)
)

lnd_id_mapping_df = lnd_id_mapping.toDF().drop(landing.IdMapping.ingestion_date.name).dropDuplicates(
    [
        landing.IdMapping.cod_sor_idi_gcr.name,
        landing.IdMapping.idi_gcr.name,
        landing.IdMapping.cod_sor_idi_src.name,
        landing.IdMapping.idi_src.name,
    ])

creat_delta_table_if_not_exists(spark, staging.IdMapping, lake_descriptor)
stg_map_path = get_s3_path(staging.IdMapping, lake_descriptor)
stg_id_mapping = DeltaTable.forPath(spark, stg_map_path)

stg_id_mapping.alias(staging.IdMapping.table_alias()).merge(
    lnd_id_mapping_df.alias(landing.IdMapping.table_alias()),
    f"""
    {landing.IdMapping.idi_gcr.column_alias()} = {staging.IdMapping.idi_gcr.column_alias()} AND
    {landing.IdMapping.idi_src.column_alias()} = {staging.IdMapping.idi_src.column_alias()} AND
    {landing.IdMapping.cod_sor_idi_src.column_alias()} = {staging.IdMapping.cod_sor_idi_src.column_alias()}
    """
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()