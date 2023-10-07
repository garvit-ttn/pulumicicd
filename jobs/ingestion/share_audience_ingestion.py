from awsglue import DynamicFrame
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from cvmdatalake import landing, creat_delta_table_if_not_exists, get_s3_path, create_dynamic_frame_from_catalog, convert_columns_to_lower_case
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

lnd_share_audience = create_dynamic_frame_from_catalog(glueContext, table=landing.ShareAudience, prefix=environment).toDF()
lnd_share_audience = convert_columns_to_lower_case(lnd_share_audience)
creat_delta_table_if_not_exists(spark, staging.ShareAudience, lake_descriptor)
stg_share_aud_path = get_s3_path(staging.ShareAudience, lake_descriptor)
lnd_share_audience.write.format("delta").mode("overwrite").save(stg_share_aud_path)
