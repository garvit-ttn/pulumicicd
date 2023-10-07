from awsglue import DynamicFrame
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import *
from delta.tables import *

# from cvmdatalake import creat_delta_table_if_not_exists, landing, staging, get_s3_path


from cvmdatalake import landing, staging, create_delta_table_from_catalog, create_dynamic_frame_from_catalog, \
    convert_columns_to_lower_case

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'lake_descriptor', 'cvm_environment'])

sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
spark = glueContext.spark_session

lake_descriptor = args['lake_descriptor']
environment = args['cvm_environment']

lnd_bu_external_id = create_dynamic_frame_from_catalog(glueContext, table=landing.BuExternalId,
                                                       prefix=environment).toDF()

# standardize columns' names
lnd_bu_external_id = convert_columns_to_lower_case(lnd_bu_external_id)

# Ingest only the latest offers
latest_ingestion_date = str(lnd_bu_external_id.select(max(col(landing.BuExternalId.ingestion_date.name))).first()[0])
print(f"Ingesting bu_external_ids for {latest_ingestion_date}")

lnd_bu_external_id = lnd_bu_external_id.filter(col(landing.BuExternalId.ingestion_date.name) == latest_ingestion_date)
print(f"There are {lnd_bu_external_id.count()} bu_external_ids to ingest")

# Deduplicate latest promotions from landing
lnd_bu_external_id: DataFrame = lnd_bu_external_id.drop(landing.BuExternalId.ingestion_date.name).dropDuplicates(
    [
        landing.BuExternalId.gcr_id.name,
        landing.BuExternalId.external_id.name,
        landing.BuExternalId.org_code.name,
    ]
)
print(f"There are {lnd_bu_external_id.count()} bu_external_ids to merge after deduplication")
lnd_bu_external_id.show(truncate=False)

# Merge latest promotions into staging by id
stg_bu_external_id = create_delta_table_from_catalog(spark, staging.BuExternalId, lake_descriptor)
stg_bu_external_id.alias(staging.BuExternalId.table_alias()).merge(
    lnd_bu_external_id.alias(landing.BuExternalId.table_alias()),
    f"""
    {landing.BuExternalId.gcr_id.column_alias()} = {staging.BuExternalId.gcr_id.column_alias()} and 
    {landing.BuExternalId.external_id.column_alias()} = {staging.BuExternalId.external_id.column_alias()} and
    {landing.BuExternalId.org_code.column_alias()} = {staging.BuExternalId.org_code.column_alias()}
    """
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# print(f"On Staging there are {stg_bu_external_id.count()} bu_external_ids after merging")