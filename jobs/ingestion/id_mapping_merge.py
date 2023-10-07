from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from delta.tables import *
from pyspark.sql.functions import *

from cvmdatalake import creat_delta_table_if_not_exists, staging, conformed, get_s3_path

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'lake_descriptor', 'cvm_environment'])

sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
spark = glueContext.spark_session

lake_descriptor = args['lake_descriptor']
environment = args['cvm_environment']

stg_map_path = get_s3_path(staging.IdMapping, lake_descriptor)
stg_id_mapping_df = DeltaTable.forPath(spark, stg_map_path).toDF()

creat_delta_table_if_not_exists(spark, conformed.IdMapping, lake_descriptor)
conformed_id_mapping_path = get_s3_path(conformed.IdMapping, lake_descriptor)
conformed_id_mapping = DeltaTable.forPath(spark, conformed_id_mapping_path)

conformed_id_mapping.alias(conformed.IdMapping.table_alias()).merge(
    stg_id_mapping_df.alias(staging.IdMapping.table_alias()),
    f"""
    {staging.IdMapping.idi_gcr.column_alias()} = {conformed.IdMapping.idi_gcr.column_alias()} AND
    {staging.IdMapping.idi_src.column_alias()} = {conformed.IdMapping.idi_src.column_alias()} AND
    {staging.IdMapping.cod_sor_idi_src.column_alias()} = {conformed.IdMapping.cod_sor_idi_src.column_alias()}
    """
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()