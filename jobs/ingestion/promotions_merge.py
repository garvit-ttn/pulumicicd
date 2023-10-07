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

conformed_promotions = create_delta_table_from_catalog(spark, conformed.Promotions, lake_descriptor)
staging_promotions_df = create_delta_table_from_catalog(spark, staging.Promotions, lake_descriptor).toDF()


staging_promotions_df: DataFrame = staging_promotions_df.dropDuplicates(
    [
        staging.Promotions.idi_offer.name
    ]
)

conformed_promotions.alias(conformed.Promotions.table_alias()).merge(
    staging_promotions_df.alias(staging.Promotions.table_alias()),
    f"""
    {staging.Promotions.idi_offer.column_alias()} = {conformed.Promotions.idi_offer.column_alias()}
    """
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
