from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import *

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

lnd_promotions = create_dynamic_frame_from_catalog(glueContext, table=landing.Promotions, prefix=environment).toDF()

# standardize columns' names
lnd_promotions = convert_columns_to_lower_case(lnd_promotions)

# Ingest only the latest offers
latest_ingestion_date = str(lnd_promotions.select(max(col(landing.Promotions.ingestion_date.name))).first()[0])
print(f"Ingesting promotions for {latest_ingestion_date}")

lnd_promotions = lnd_promotions.filter(col(landing.Promotions.ingestion_date.name) == latest_ingestion_date)
print(f"There are {lnd_promotions.count()} promotions to ingest")

# Deduplicate latest promotions from landing
lnd_promotions: DataFrame = lnd_promotions.drop(landing.Promotions.ingestion_date.name).dropDuplicates(
    [
        landing.Promotions.idi_offer.name
    ]
)
print(f"There are {lnd_promotions.count()} promotions to merge after deduplication")
lnd_promotions.show(truncate=False)

# Merge latest promotions into staging by id
stg_promotions = create_delta_table_from_catalog(spark, staging.Promotions, lake_descriptor)
stg_promotions.alias(staging.Promotions.table_alias()).merge(
    lnd_promotions.alias(landing.Promotions.table_alias()),
    f"""
    {landing.Promotions.idi_offer.column_alias()} = {staging.Promotions.idi_offer.column_alias()}
    """
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
