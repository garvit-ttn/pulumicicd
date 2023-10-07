from awsglue import DynamicFrame
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import *
from delta.tables import *
import boto3
from botocore.exceptions import ClientError
import datetime


from cvmdatalake import creat_delta_table_if_not_exists, landing, staging, get_s3_path, convert_columns_to_lower_case


def retrieves_details_of_bookmarked_job(bookmarked_job_name):
   session = boto3.session.Session()
   glue_client = session.client('glue')
   try:
      response = glue_client.get_job_bookmark(JobName=bookmarked_job_name)
      return response
   except ClientError as e:
      raise Exception("boto3 client error in retrieves_details_of_bookmarked_job: " + e.__str__())
   except Exception as e:
      raise Exception("Unexpected error in retrieves_details_of_bookmarked_job: " + e.__str__())


args = getResolvedOptions(sys.argv, ['JOB_NAME', 'lake_descriptor', 'cvm_environment'])

sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
spark = glueContext.spark_session

lake_descriptor = args['lake_descriptor']
environment = args['cvm_environment']

lnd_transactions: DataFrame = glueContext.create_dynamic_frame.from_catalog(
    database=landing.Transactions.catalog_db_name(environment),
    table_name=landing.Transactions.catalog_table_name(environment),
    transformation_ctx=landing.Transactions.table_id(),
    additional_options={"mergeSchema": "true"}
).toDF()

df1d = lnd_transactions.select(date_sub(max(landing.Transactions.ingestion_date.name),2)).alias("one_day")
two_day_ago = df1d.collect()[0][0]
print("two_day_ago")
print(two_day_ago)


lnd_transactions = lnd_transactions.filter(col(landing.Transactions.ingestion_date.name) >= two_day_ago )
# lnd_transactions = lnd_transactions.filter(col(landing.Transactions.bu.name) != 'share')

# print(retrieves_details_of_bookmarked_job("cvm_prod_ingestion_transactions_ingestion_glue_job-1b26371"))


# min_date = lnd_transactions.select(min(landing.Transactions.ingestion_date.name)).first()[0]
# max_date = lnd_transactions.select(max(landing.Transactions.ingestion_date.name)).first()[0]

# print("ingestion_date - MIN")
# print(min_date)
# print("ingestion_date - MAX")
# print(max_date)

lnd_transactions = convert_columns_to_lower_case(lnd_transactions)

creat_delta_table_if_not_exists(spark, staging.Transactions, lake_descriptor)
stg_trans_path = get_s3_path(staging.Transactions, lake_descriptor)
staging_Transactions = DeltaTable.forPath(spark, stg_trans_path)



lnd_transactions = lnd_transactions.coalesce(100)


#  Find the maximum DAT_BATCH for each IDI_TURNOVER
max_dates_df = lnd_transactions.groupBy('IDI_TURNOVER').agg(max('DAT_BATCH').alias('MAX_DAT_BATCH')).withColumnRenamed('IDI_TURNOVER','MAX_IDI_TURNOVER')
cols = ("MAX_DAT_BATCH", "MAX_IDI_TURNOVER")
# Join the max_dates_df with the original DataFrame to get the corresponding OTHER_COLUMN
lnd_transactions = lnd_transactions.join(max_dates_df, (lnd_transactions['IDI_TURNOVER'] == max_dates_df['MAX_IDI_TURNOVER']) & (lnd_transactions['DAT_BATCH'] == max_dates_df['MAX_DAT_BATCH']), 'inner').drop(*cols)

lnd_transactions: DataFrame = lnd_transactions.dropDuplicates(
    [
        f"{landing.Transactions.idi_turnover.name}",
        f"{landing.Transactions.bu.name}"
    ]
)




# dat_date_type_1 IN ( cast('2019-10-14' as date ) , cast('2019-10-13' as date ) )
# distinct_date_type_1_list = lnd_transactions.select("dat_date_type_1").distinct().rdd.map(lambda row: row[0]).collect()
# print(distinct_date_type_1_list)
distinct_date_type_1_df = lnd_transactions.select("dat_date_type_1").distinct()
distinct_date_type_1_list = [row[0] for row in distinct_date_type_1_df.collect()]
string_dates = [date.strftime('cast(\'%Y-%m-%d\' as date)') for date in distinct_date_type_1_list]
separator = ', '
string_dates_in = separator.join(string_dates)
string_dates_in = f' IN ( {string_dates_in} )'

print(string_dates_in)



distinct_bu_df = lnd_transactions.select("bu").distinct()
distinct_bu_list = [row[0] for row in distinct_bu_df.collect()]
distinct_bu_list_in = [f'\'{bu}\'' for bu in distinct_bu_list]
distinct_bu_list_in = separator.join(distinct_bu_list_in)
distinct_bu_list_in = f' IN ( {distinct_bu_list_in} )'

print(distinct_bu_list_in)



staging_Transactions.alias(staging.Transactions.table_alias()).merge(
    lnd_transactions.alias(landing.Transactions.table_alias()),
    f"""
    {staging.Transactions.idi_turnover.column_alias()} = {landing.Transactions.idi_turnover.column_alias()} AND
    {staging.Transactions.bu.column_alias()} {distinct_bu_list_in} AND
    {staging.Transactions.dat_date_type_1.column_alias()} {string_dates_in}
    """
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# for historical reasons
#
# #Vipin fix -1-
#
# condition = col("DES_COMPANY") == "fyua"
# update_expr_des_maf_brand_name = when(condition, "THAT").otherwise(col("DES_MAF_BRAND_NAME"))
# update_expr_idi_brand_code = when(condition, "THAT").otherwise(col("IDI_Brand_Code"))
#
# # Apply the updates to the DataFrame
# lnd_transactions = lnd_transactions.withColumn("DES_MAF_BRAND_NAME", update_expr_des_maf_brand_name) \
#               .withColumn("IDI_Brand_Code", update_expr_idi_brand_code)
#
#
# #Vipin fix -2-
# lnd_transactions = lnd_transactions.withColumn("CDE_DATE_KEY", date_format(to_date(col("CDE_DATE_KEY"), "yyyy-MM-dd"), "yyyyMMdd"))
#
#
#
#
# #Vipin fix -3-
#
# folder_path = "s3://cvm-prod-landing-5d6c06b/archive/backup_share_txn_2023_06_02/2023-06-01_1/"
# share = spark.read.parquet(folder_path)
# share = share.withColumn("bu", lit("share"))
# share = convert_columns_to_lower_case(share)
#
# staging_Transactions.alias(staging.Transactions.table_alias()).merge(
#     share.alias(landing.Transactions.table_alias()),
#     f"""
#     {landing.Transactions.idi_turnover.column_alias()} = {staging.Transactions.idi_turnover.column_alias()}
#     """
# ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
#
# #Vipin fix -4-
# folder_path = "s3://cvm-prod-landing-5d6c06b/archive/backup_share_txn_2023_06_16/2023-06-15_1/"
# share = spark.read.parquet(folder_path)
# share = share.withColumn("bu", lit("share"))
# share = convert_columns_to_lower_case(share)
#
# staging_Transactions.alias(staging.Transactions.table_alias()).merge(
#     share.alias(landing.Transactions.table_alias()),
#     f"""
#     {landing.Transactions.idi_turnover.column_alias()} = {staging.Transactions.idi_turnover.column_alias()}
#     """
# ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()