from awsglue import DynamicFrame
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import *
from pyspark.sql.types import *

from cvmdatalake import creat_delta_table_if_not_exists, landing, staging, get_s3_path

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'lake_descriptor', 'cvm_environment'])

sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
spark = glueContext.spark_session

lake_descriptor = args['lake_descriptor']
environment = args['cvm_environment']

lnd_activity: DynamicFrame = glueContext.create_dynamic_frame.from_catalog(
    database=landing.Activities.catalog_db_name(environment),
    table_name=landing.Activities.catalog_table_name(environment),
    transformation_ctx=landing.Activities.table_id()
)

lnd_activity_df = lnd_activity.toDF().drop(landing.Activities.ingestion_date.name).dropDuplicates(
    [
        landing.Activities.idi_activity.name
    ])
    
lnd_activity_df= lnd_activity_df.withColumn("dat_activity",
lnd_activity_df["dat_activity"].cast(StringType()))

creat_delta_table_if_not_exists(spark, staging.Activities, lake_descriptor)
stg_map_path = get_s3_path(staging.Activities, lake_descriptor)
lnd_activity_df.write \
    .partitionBy('bu', 'dat_activity') \
    .format('delta') \
    .mode('overwrite') \
    .save(stg_map_path)