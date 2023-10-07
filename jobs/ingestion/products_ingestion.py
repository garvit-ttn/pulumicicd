from awsglue import DynamicFrame
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import *

from cvmdatalake import landing, staging, landing_to_staging_full_load_overwrite

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'lake_descriptor', 'cvm_environment'])

sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
spark = glueContext.spark_session

lake_descriptor = args['lake_descriptor']
environment = args['cvm_environment']

landing_to_staging_full_load_overwrite(glueContext, lnd_table=landing.Products, stg_table=staging.Products, on_primary_key='idi_proposition', drop_duplicates_on_primary=True, prefix=environment, lake_descriptor=lake_descriptor)