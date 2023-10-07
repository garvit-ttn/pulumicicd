from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import *

from cvmdatalake import staging, conformed, staging_to_conformed_full_load

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'lake_descriptor', 'cvm_environment'])

sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
spark = glueContext.spark_session

lake_descriptor = args['lake_descriptor']
environment = args['cvm_environment']

staging_to_conformed_full_load(glueContext, stg_table=staging.LoyaltyBusinessUnit, conf_table=conformed.LoyaltyBusinessUnit, on_primary_key='bu_key', lake_descriptor=lake_descriptor)
staging_to_conformed_full_load(glueContext, stg_table=staging.LoyaltyCountry, conf_table=conformed.LoyaltyCountry, on_primary_key="country_code",  lake_descriptor=lake_descriptor)
staging_to_conformed_full_load(glueContext, stg_table=staging.LoyaltyOpco, conf_table=conformed.LoyaltyOpco, on_primary_key="opco_key",  lake_descriptor=lake_descriptor)
staging_to_conformed_full_load(glueContext, stg_table=staging.LoyaltySponsor, conf_table=conformed.LoyaltySponsor, on_primary_key="sponsor_key", lake_descriptor=lake_descriptor)
staging_to_conformed_full_load(glueContext, stg_table=staging.PtaSta, conf_table=conformed.PtaSta, on_primary_key="idi_counterparty_gr", lake_descriptor=lake_descriptor)