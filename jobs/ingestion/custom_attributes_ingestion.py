from awsglue import DynamicFrame
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import ApplyMapping
from awsglue.utils import getResolvedOptions
from delta.tables import *
from pyspark.sql.functions import *
import re


def move_numbers_to_end(name: str):
    name = name.lower()
    name = name.replace("ca_","")

    # Extract the numbers at the beginning of the string
    match = re.match(r'^(\d+)(_*)(.*)$', name)
    if match:
        numbers = match.group(1)
        underscore = match.group(2)
        remaining_string = match.group(3)
        return remaining_string + underscore + numbers
    else:
        return name

from cvmdatalake import creat_delta_table_if_not_exists, landing, staging, get_s3_path

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'lake_descriptor', 'cvm_environment'])

sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
spark = glueContext.spark_session

lake_descriptor = args['lake_descriptor']
environment = args['cvm_environment']

lnd_cust_attribs_raw: DynamicFrame = glueContext.create_dynamic_frame.from_catalog(
    database=landing.CustomAttributes.catalog_db_name(environment),
    table_name=landing.CustomAttributes.catalog_table_name(environment)
)


print("1- schema")
print(lnd_cust_attribs_raw.printSchema())


lnd_cust_attribs: DataFrame = ApplyMapping.apply(
    frame=lnd_cust_attribs_raw,
    mappings=[
        (c.name, c.data_type, move_numbers_to_end(c.name), c.data_type)
        for c in landing.CustomAttributes
    ]
).toDF()

print("2- schema")
print(lnd_cust_attribs.printSchema())
print(lnd_cust_attribs.count())

lnd_cust_attribs = lnd_cust_attribs.coalesce(100)


lnd_cust_attribs = (
    lnd_cust_attribs
    .drop(landing.CustomAttributes.partition_0.name)
    .withColumnRenamed("idi_counterparty_gr", "idi_counterparty")
    .filter(col("idi_counterparty") != '')
)

print("3- schema")
print(lnd_cust_attribs.count())


max_dates_df = lnd_cust_attribs.groupBy('IDI_COUNTERPARTY').agg(max('DAT_BATCH').alias('MAX_DAT_BATCH')).withColumnRenamed('IDI_COUNTERPARTY','MAX_IDI_COUNTERPARTY')
cols = ("MAX_DAT_BATCH", "MAX_IDI_COUNTERPARTY")
lnd_cust_attribs = lnd_cust_attribs.join(max_dates_df, (lnd_cust_attribs['IDI_COUNTERPARTY'] == max_dates_df['MAX_IDI_COUNTERPARTY']) & (lnd_cust_attribs['DAT_BATCH'] == max_dates_df['MAX_DAT_BATCH']), 'inner').drop(*cols)



lnd_cust_attribs = lnd_cust_attribs.dropDuplicates(["IDI_COUNTERPARTY"])



creat_delta_table_if_not_exists(spark, staging.CustomAttributes, lake_descriptor)
stg_map_path = get_s3_path(staging.CustomAttributes, lake_descriptor)


lnd_cust_attribs.write.format("delta").mode("overwrite").save(stg_map_path)

# stg_cust_attribs = DeltaTable.forPath(spark, stg_map_path)
# stg_cust_attribs.alias(staging.CustomAttributes.table_alias()).merge(
#     lnd_cust_attribs.alias(landing.CustomAttributes.table_alias()),
#     f"""
#     {landing.CustomAttributes.table_alias()+".idi_counterparty"} = {staging.CustomAttributes.idi_counterparty.column_alias()}
#     """
# ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
