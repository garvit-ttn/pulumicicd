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

stg_act_path = get_s3_path(staging.Activities, lake_descriptor)
stg_activities_df = DeltaTable.forPath(spark, stg_act_path).toDF()

creat_delta_table_if_not_exists(spark, conformed.Activities, lake_descriptor)
conformed_activities_path = get_s3_path(conformed.Activities, lake_descriptor)
conformed_activities = DeltaTable.forPath(spark, conformed_activities_path)

conformed_activities.alias(conformed.Activities.table_alias()).merge(
    stg_activities_df.alias(staging.Activities.table_alias()),
    f"""
    {staging.Activities.idi_activity.column_alias()} = {conformed.Activities.idi_activity.column_alias()}
    """
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()


# stg_activities_df.write \
#     .partitionBy('bu', 'dat_activity') \
#     .format('delta') \
#     .mode('append') \
#     .save(conformed_activities_path)


# conformed_activities.alias(conformed.Activities.table_alias()).merge(
#     stg_activities_df.alias(staging.Activities.table_alias()),
#     f"""
#     {staging.Activities.dat_batch.column_alias()} = {conformed.Activities.dat_batch.column_alias()} AND
#     {staging.Activities.idi_activity.column_alias()} = {conformed.Activities.idi_activity.column_alias()} AND
#     {staging.Activities.cod_sor_activity.column_alias()} = {conformed.Activities.cod_sor_activity.column_alias()} AND
#     {staging.Activities.ind_counterparty_type.column_alias()} = {conformed.Activities.ind_counterparty_type.column_alias()} AND
#     """
# ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
