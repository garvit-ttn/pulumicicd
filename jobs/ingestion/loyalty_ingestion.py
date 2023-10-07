from awsglue import DynamicFrame
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import *

from cvmdatalake import landing, staging, landing_to_staging_full_load

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'lake_descriptor', 'cvm_environment'])

sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
spark = glueContext.spark_session

lake_descriptor = args['lake_descriptor']
environment = args['cvm_environment']

landing_to_staging_full_load(glueContext, lnd_table=landing.LoyaltyBusinessUnit, stg_table=staging.LoyaltyBusinessUnit, on_primary_key='bu_key', prefix=environment, lake_descriptor=lake_descriptor)
landing_to_staging_full_load(glueContext, lnd_table=landing.LoyaltyCountry, stg_table=staging.LoyaltyCountry, on_primary_key="country_code", prefix=environment, lake_descriptor=lake_descriptor)
landing_to_staging_full_load(glueContext, lnd_table=landing.LoyaltyOpco, stg_table=staging.LoyaltyOpco, on_primary_key="opco_key", prefix=environment, lake_descriptor=lake_descriptor)
landing_to_staging_full_load(glueContext, lnd_table=landing.LoyaltySponsor, stg_table=staging.LoyaltySponsor, on_primary_key="sponsor_key", prefix=environment, lake_descriptor=lake_descriptor)
landing_to_staging_full_load(glueContext, lnd_table=landing.PtaSta, stg_table=staging.PtaSta, on_primary_key="idi_counterparty_gr", prefix=environment, lake_descriptor=lake_descriptor)