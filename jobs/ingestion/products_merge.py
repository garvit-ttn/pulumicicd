from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from delta.tables import *
from pyspark.sql.functions import *

from cvmdatalake import staging, conformed, staging_to_conformed_full_load_overwrite

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'lake_descriptor', 'cvm_environment'])

sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
spark = glueContext.spark_session

lake_descriptor = args['lake_descriptor']
environment = args['cvm_environment']


staging_to_conformed_full_load_overwrite(glueContext, stg_table=staging.Products, conf_table=conformed.Products, on_primary_key='idi_proposition', drop_duplicates_on_primary=True, lake_descriptor=lake_descriptor)