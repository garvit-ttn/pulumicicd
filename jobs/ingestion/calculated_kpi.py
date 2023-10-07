import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from awsglue import DynamicFrame
from awsglue.transforms import *
from delta import DeltaTable
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.functions import col
import pyspark.sql.functions as func
from pyspark.sql.window import Window
from pyspark.sql.types import *


from cvmdatalake import conformed, staging, creat_delta_table_if_not_exists, get_s3_path, nullify_empty_cells

def monteary_brand_add(bu: str, sponsor: str, months_ago , alias , gcr_joined_trx_FRTM_2) -> DataFrame:
    conformed_trans_path = get_s3_path(conformed.Transactions, lake_descriptor)

    trx_ni = (
        DeltaTable.forPath(spark, conformed_trans_path).toDF()
        .filter(col(conformed.Transactions.cde_base_sponsor_key.name) == sponsor)
        .filter(col(conformed.Transactions.dat_date_type_1.name) >= '2022-01-02')
        .filter(col(conformed.Transactions.bu.name) == bu)
    )
    trx_MONET_3M_ni = (
        trx_ni.filter(F.col(conformed.Transactions.dat_date_type_1.name) >= months_ago)
        .groupBy(conformed.Transactions.idi_counterparty_gr.name)
        .agg(sum(conformed.Transactions.cua_amount_type_1.name).alias(alias))
    )
    gcr_joined_trx_FRTM_2 = gcr_joined_trx_FRTM_2.join(trx_MONET_3M_ni, conformed.Transactions.idi_counterparty_gr.name,'left')

    return gcr_joined_trx_FRTM_2

def monteary_mall_add(bu: str, sponsor: str, months_ago , alias , gcr_joined_trx_FRTM_2) -> DataFrame:
    conformed_trans_path = get_s3_path(conformed.Transactions, lake_descriptor)

    trx_ni = (
        DeltaTable.forPath(spark, conformed_trans_path).toDF()
        .filter(col(conformed.Transactions.des_store_name.name) == sponsor)
        .filter(col(conformed.Transactions.dat_date_type_1.name) >= '2022-01-02')
        .filter(col(conformed.Transactions.bu.name) == bu)
    )
    trx_MONET_3M_ni = (
        trx_ni.filter(F.col(conformed.Transactions.dat_date_type_1.name) >= months_ago)
        .groupBy(conformed.Transactions.idi_counterparty_gr.name)
        .agg(sum(conformed.Transactions.cua_amount_type_1.name).alias(alias))
    )
    gcr_joined_trx_FRTM_2 = gcr_joined_trx_FRTM_2.join(trx_MONET_3M_ni, conformed.Transactions.idi_counterparty_gr.name,'left')

    return gcr_joined_trx_FRTM_2

def monteary_bu_add(bu: str, months_ago , alias , gcr_joined_trx_FRTM_2) -> DataFrame:
    conformed_trans_path = get_s3_path(conformed.Transactions, lake_descriptor)

    trx_ni = (
        DeltaTable.forPath(spark, conformed_trans_path).toDF()
        .filter(col(conformed.Transactions.bu.name) == bu)
        .filter(col(conformed.Transactions.dat_date_type_1.name) >= '2022-01-02')
    )
    trx_MONET_3M_ni = (
        trx_ni.filter(F.col(conformed.Transactions.dat_date_type_1.name) >= months_ago)
        .groupBy(conformed.Transactions.idi_counterparty_gr.name)
        .agg(sum(conformed.Transactions.cua_amount_type_1.name).alias(alias))
    )
    gcr_joined_trx_FRTM_2 = gcr_joined_trx_FRTM_2.join(trx_MONET_3M_ni, conformed.Transactions.idi_counterparty_gr.name,'left')

    return gcr_joined_trx_FRTM_2

def monteary_custom_add(bu: str, sponsor: str, months_ago , alias , gcr_joined_trx_FRTM_2) -> DataFrame:
    conformed_trans_path = get_s3_path(conformed.Transactions, lake_descriptor)

    trx_ni = DeltaTable.forPath(spark, conformed_trans_path).toDF()

    if sponsor == 'smbu':
        trx_ni = DeltaTable.forPath(spark, conformed_trans_path).toDF()
        trx_ni = nullify_empty_cells(trx_ni)
        trx_ni = (
             trx_ni.filter(col(conformed.Transactions.cde_base_bu_key.name) == '290')
             .filter(col(conformed.Transactions.cde_layered_bu_key.name).isNull())
             .filter(col(conformed.Transactions.dat_date_type_1.name) >= '2022-01-02')
             .filter(col(conformed.Transactions.bu.name) == bu)
        )

    if sponsor == 'lec':
        trx_ni = (
            DeltaTable.forPath(spark, conformed_trans_path).toDF()
            .filter(col(conformed.Transactions.cde_base_bu_key.name) == '289')
            .filter(col(conformed.Transactions.dat_date_type_1.name) >= '2022-01-02')
            .filter(col(conformed.Transactions.bu.name) == bu)
        )

    if sponsor == 'lifestyle':
        trx_ni = (
            DeltaTable.forPath(spark, conformed_trans_path).toDF()
            .filter(col(conformed.Transactions.cde_base_bu_key.name) == '291')
            .filter(col(conformed.Transactions.dat_date_type_1.name) >= '2022-01-02')
            .filter(col(conformed.Transactions.bu.name) == bu)
        )

    if sponsor == 'partner':
        sponsors = ["72", "78", "79", "92", "94", "98", "101"]
        trx_ni = (
            DeltaTable.forPath(spark, conformed_trans_path).toDF()
            .filter(col(conformed.Transactions.cde_base_sponsor_key.name).isin(sponsors))
            .filter(col(conformed.Transactions.dat_date_type_1.name) >= '2022-01-02')
            .filter(col(conformed.Transactions.bu.name) == bu)
        )

    if sponsor == 'Mall of the Emirates':
        trx_ni = (
            DeltaTable.forPath(spark, conformed_trans_path).toDF()
            .filter(col(conformed.Transactions.des_mall_name.name) == sponsor)
            .filter(col(conformed.Transactions.dat_date_type_1.name) >= '2022-01-02')
            .filter(col(conformed.Transactions.bu.name) == bu)
        )

    if sponsor == 'City Centre Deira':
        trx_ni = (
            DeltaTable.forPath(spark, conformed_trans_path).toDF()
            .filter(col(conformed.Transactions.des_mall_name.name) == sponsor)
            .filter(col(conformed.Transactions.dat_date_type_1.name) >= '2022-01-02')
            .filter(col(conformed.Transactions.bu.name) == bu)
        )

    trx_MONET_3M_ni = (
        trx_ni.filter(F.col(conformed.Transactions.dat_date_type_1.name) >= months_ago)
        .groupBy(conformed.Transactions.idi_counterparty_gr.name)
        .agg(sum(conformed.Transactions.cua_amount_type_1.name).alias(alias))
    )
    gcr_joined_trx_FRTM_2 = gcr_joined_trx_FRTM_2.join(trx_MONET_3M_ni, conformed.Transactions.idi_counterparty_gr.name,'left')

    return gcr_joined_trx_FRTM_2

def monteary_lifestyle_brand_add(sponsor: str, months_ago , alias , gcr_joined_trx_FRTM_2) -> DataFrame:
    conformed_trans_path = get_s3_path(conformed.Transactions, lake_descriptor)

    trx_ni = (
        DeltaTable.forPath(spark, conformed_trans_path).toDF()
        .filter(col(conformed.Transactions.bu.name) == sponsor)
        .filter(col(conformed.Transactions.dat_date_type_1.name) >= '2022-01-02')
        .filter(col(conformed.Transactions.bu.name) == 'fsn_new')
    )

    trx_MONET_3M_ni = (
        trx_ni.filter(F.col(conformed.Transactions.dat_date_type_1.name) >= months_ago)
        .groupBy(conformed.Transactions.idi_counterparty_gr.name)
        .agg(sum(conformed.Transactions.cua_amount_type_1.name).alias(alias))
    )
    gcr_joined_trx_FRTM_2 = gcr_joined_trx_FRTM_2.join(trx_MONET_3M_ni, conformed.Transactions.idi_counterparty_gr.name,'left')

    return gcr_joined_trx_FRTM_2

def total_transactions_custom_add(bu: str, sponsor: str, months_ago , alias , gcr_joined_trx_FRTM_2) -> DataFrame:
    conformed_trans_path = get_s3_path(conformed.Transactions, lake_descriptor)

    if bu == 'share':
        trx_ni = (
            DeltaTable.forPath(spark, conformed_trans_path).toDF()
            .filter(col(conformed.Transactions.cde_base_sponsor_key.name) == sponsor)
            .filter(col(conformed.Transactions.dat_date_type_1.name) >= months_ago)
            .filter(col(conformed.Transactions.bu.name) == bu)
        )

    if bu == 'fsn_new':
        trx_ni = (
            DeltaTable.forPath(spark, conformed_trans_path).toDF()
            .filter(col(conformed.Transactions.des_country_name.name) == 'UAE')
            .filter(col(conformed.Transactions.des_maf_brand_name.name) == sponsor)
            .filter(col(conformed.Transactions.dat_date_type_1.name) >= months_ago)
            .filter(col(conformed.Transactions.bu.name) == bu)
        )

    if sponsor == 'partner':
        sponsors = ["72", "78", "79", "92", "94", "98", "101"]
        trx_ni = (
            DeltaTable.forPath(spark, conformed_trans_path).toDF()
            .filter(col(conformed.Transactions.cde_base_sponsor_key.name).isin(sponsors))
            .filter(col(conformed.Transactions.dat_date_type_1.name) >= '2022-01-02')
        )

    trx_MONET_3M_ni = (
        trx_ni.filter(F.col(conformed.Transactions.dat_date_type_1.name) >= months_ago)
        .groupBy(conformed.Transactions.idi_counterparty_gr.name)
        .agg(count(conformed.Transactions.idi_counterparty_gr.name).alias(alias))
    )
    gcr_joined_trx_FRTM_2 = gcr_joined_trx_FRTM_2.join(trx_MONET_3M_ni, conformed.Transactions.idi_counterparty_gr.name,'left')

    return gcr_joined_trx_FRTM_2

def find_first_last_trx_date_custom_add(sponsor: str, first_or_last: str, alias, gcr_joined_trx_FRTM_2) -> DataFrame:
    conformed_trans_path = get_s3_path(conformed.Transactions, lake_descriptor)

    trx_ni = (
        DeltaTable.forPath(spark, conformed_trans_path).toDF()
        .filter(col(conformed.Transactions.cde_base_sponsor_key.name) == sponsor)
        .filter(col(conformed.Transactions.dat_date_type_1.name) >= '2022-01-02')
    )
    print("trx_ni.count()")
    print(trx_ni.count())

    if sponsor == 'partner':
        sponsors = ["72", "78", "79", "92", "94", "98", "101"]
        trx_ni = (
            DeltaTable.forPath(spark, conformed_trans_path).toDF()
            .filter(col(conformed.Transactions.cde_base_sponsor_key.name).isin(sponsors))
            .filter(col(conformed.Transactions.dat_date_type_1.name) >= '2022-01-02')
        )

    if first_or_last == 'first':
        window_spec = Window.partitionBy(conformed.Transactions.idi_counterparty_gr.name).orderBy(F.col(conformed.Transactions.dat_date_type_1.name))
    if first_or_last == 'last':
        window_spec = Window.partitionBy(conformed.Transactions.idi_counterparty_gr.name).orderBy(F.col(conformed.Transactions.dat_date_type_1.name).desc())

    trx_MONET_3M_ni = (
        trx_ni
        .withColumn("row_num", F.row_number().over(window_spec))  # Add a row number column
        .filter(F.col("row_num") == 1)  # Filter the first transaction for each counterparty
        .select(conformed.Transactions.idi_counterparty_gr.name,
                col(conformed.Transactions.dat_date_type_1.name).cast(DateType()).alias(alias))  # Select the relevant columns
        .drop("row_num")  # Drop the row number column
    )

    gcr_joined_trx_FRTM_2 = gcr_joined_trx_FRTM_2.join(trx_MONET_3M_ni, conformed.Transactions.idi_counterparty_gr.name,'left')

    return gcr_joined_trx_FRTM_2

def engagement_status_custom_add(bu: str, sponsor: str, alias, gcr_joined_trx_FRTM_2, months_ago) -> DataFrame:
    conformed_trans_path = get_s3_path(conformed.Transactions, lake_descriptor)

    if bu == 'share':
        trx_ni = (
            DeltaTable.forPath(spark, conformed_trans_path).toDF()
            .filter(col(conformed.Transactions.cde_base_sponsor_key.name) == sponsor)
            .filter(col(conformed.Transactions.dat_date_type_1.name) >= months_ago)
            .filter(col(conformed.Transactions.bu.name) == bu)
        )

    if bu == 'fsn_new':
        trx_ni = (
            DeltaTable.forPath(spark, conformed_trans_path).toDF()
            .filter(col(conformed.Transactions.des_country_name.name) == 'UAE')
            .filter(col(conformed.Transactions.des_maf_brand_name.name) == sponsor)
            .filter(col(conformed.Transactions.dat_date_type_1.name) >= months_ago)
            .filter(col(conformed.Transactions.bu.name) == bu)
        )

    if sponsor == 'partner':
        sponsors = ["72", "78", "79", "92", "94", "98", "101"]
        trx_ni = (
            DeltaTable.forPath(spark, conformed_trans_path).toDF()
            .filter(col(conformed.Transactions.cde_base_sponsor_key.name).isin(sponsors))
            .filter(col(conformed.Transactions.dat_date_type_1.name) >= months_ago)
        )

    window_spec = Window.partitionBy(conformed.Transactions.idi_counterparty_gr.name)

    trx_MONET_3M_ni = (
        trx_ni
        .withColumn("has_transaction", F.when(F.count(conformed.Transactions.idi_counterparty_gr.name).over(window_spec) > 0, 1).otherwise(0))
        .select(conformed.Transactions.idi_counterparty_gr.name, F.col("has_transaction").alias(alias))
        .distinct()  # Remove duplicate counterparty entries
    )

    gcr_joined_trx_FRTM_2 = gcr_joined_trx_FRTM_2.join(trx_MONET_3M_ni, conformed.Transactions.idi_counterparty_gr.name, 'left')

    return gcr_joined_trx_FRTM_2

def engagement_status_custom_inactive_add(bu: str, sponsor: str, alias, gcr_joined_trx_FRTM_2, months_ago) -> DataFrame:
    conformed_trans_path = get_s3_path(conformed.Transactions, lake_descriptor)

    if bu == 'share':
        trx_ni = (
            DeltaTable.forPath(spark, conformed_trans_path).toDF()
            .filter(col(conformed.Transactions.cde_base_sponsor_key.name) == sponsor)
            .filter(col(conformed.Transactions.dat_date_type_1.name) >= months_ago)
            .filter(col(conformed.Transactions.bu.name) == bu)
        )

    if bu == 'fsn_new':
        trx_ni = (
            DeltaTable.forPath(spark, conformed_trans_path).toDF()
            .filter(col(conformed.Transactions.des_country_name.name) == 'UAE')
            .filter(col(conformed.Transactions.des_maf_brand_name.name) == sponsor)
            .filter(col(conformed.Transactions.dat_date_type_1.name) >= months_ago)
            .filter(col(conformed.Transactions.bu.name) == bu)
        )

    if sponsor == 'partner':
        sponsors = ["72", "78", "79", "92", "94", "98", "101"]
        trx_ni = (
            DeltaTable.forPath(spark, conformed_trans_path).toDF()
            .filter(col(conformed.Transactions.cde_base_sponsor_key.name).isin(sponsors))
            .filter(col(conformed.Transactions.dat_date_type_1.name) >= months_ago)
        )

    window_spec = Window.partitionBy(conformed.Transactions.idi_counterparty_gr.name)

    trx_MONET_3M_ni = (
        trx_ni
        .withColumn("has_transaction", F.when(F.count(conformed.Transactions.idi_counterparty_gr.name).over(window_spec) == 0, 1).otherwise(0))
        .select(conformed.Transactions.idi_counterparty_gr.name, F.col("has_transaction").alias(alias))
        .distinct()  # Remove duplicate counterparty entries
    )

    gcr_joined_trx_FRTM_2 = gcr_joined_trx_FRTM_2.join(trx_MONET_3M_ni, conformed.Transactions.idi_counterparty_gr.name, 'left')

    return gcr_joined_trx_FRTM_2

def average_customer_spend_brand_add(bu: str, sponsor: str, months_ago , alias , gcr_joined_trx_FRTM_2) -> DataFrame:
    conformed_trans_path = get_s3_path(conformed.Transactions, lake_descriptor)

    if bu == 'share':
        trx_ni = (
            DeltaTable.forPath(spark, conformed_trans_path).toDF()
            .filter(col(conformed.Transactions.cde_base_sponsor_key.name) == sponsor)
            .filter(col(conformed.Transactions.dat_date_type_1.name) >= months_ago)
            .filter(col(conformed.Transactions.bu.name) == bu)
        )

    if bu == 'fsn_new':
        trx_ni = (
            DeltaTable.forPath(spark, conformed_trans_path).toDF()
            .filter(col(conformed.Transactions.des_country_name.name) == 'UAE')
            .filter(col(conformed.Transactions.des_maf_brand_name.name) == sponsor)
            .filter(col(conformed.Transactions.dat_date_type_1.name) >= months_ago)
            .filter(col(conformed.Transactions.bu.name) == bu)
        )

    trx_MONET_3M_ni = (
        trx_ni.filter(F.col(conformed.Transactions.dat_date_type_1.name) >= months_ago)
        .groupBy((conformed.Transactions.idi_counterparty_gr.name)).agg(avg(conformed.Transactions.cua_amount_type_1.name).alias(alias))
    )
    gcr_joined_trx_FRTM_2 = gcr_joined_trx_FRTM_2.join(trx_MONET_3M_ni, conformed.Transactions.idi_counterparty_gr.name,'left')

    return gcr_joined_trx_FRTM_2


def avg_transaction_value_brand_add(bu: str, sponsor: str, months_ago , alias , gcr_joined_trx_FRTM_2) -> DataFrame:
    conformed_trans_path = get_s3_path(conformed.Transactions, lake_descriptor)

    if bu == 'share':
        trx_ni = (
            DeltaTable.forPath(spark, conformed_trans_path).toDF()
            .filter(col(conformed.Transactions.cde_base_sponsor_key.name) == sponsor)
            .filter(col(conformed.Transactions.dat_date_type_1.name) >= months_ago)
            .filter(col(conformed.Transactions.bu.name) == bu)
        )

    if bu == 'fsn_new':
        trx_ni = (
            DeltaTable.forPath(spark, conformed_trans_path).toDF()
            .filter(col(conformed.Transactions.des_country_name.name) == 'UAE')
            .filter(col(conformed.Transactions.des_maf_brand_name.name) == sponsor)
            .filter(col(conformed.Transactions.dat_date_type_1.name) >= months_ago)
            .filter(col(conformed.Transactions.bu.name) == bu)
        )

    customer_total_spend_df = (
        trx_ni.filter(F.col(conformed.Transactions.dat_date_type_1.name) >= months_ago)
        .groupBy((conformed.Transactions.idi_counterparty_gr.name)).agg(sum(conformed.Transactions.cua_amount_type_1.name).alias("total_spend"), count("*").alias("total_transactions"))
    )

    trx_MONET_3M_ni = (
        trx_ni.filter(F.col(conformed.Transactions.dat_date_type_1.name) >= months_ago)
        .groupBy((conformed.Transactions.idi_counterparty_gr.name)).agg(avg(conformed.Transactions.cua_amount_type_1.name).alias(alias))
    )


    gcr_joined_trx_FRTM_2 = gcr_joined_trx_FRTM_2.join(trx_MONET_3M_ni, conformed.Transactions.idi_counterparty_gr.name,'left')

    return gcr_joined_trx_FRTM_2


args = getResolvedOptions(sys.argv, ['JOB_NAME', 'lake_descriptor', 'cvm_environment'])

sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
spark = glueContext.spark_session

lake_descriptor = args['lake_descriptor']
cvm_environment = args['cvm_environment']

# MONETARY

conformed_trans_path = get_s3_path(conformed.Transactions, lake_descriptor)
trx = DeltaTable.forPath(spark, conformed_trans_path).toDF().filter(col(conformed.Transactions.bu.name) == 'share').filter(col(conformed.Transactions.dat_date_type_1.name) >= '2022-01-02')

df1d = trx.select(date_sub(max(trx.dat_date_type_1),1)).alias("one_day")
df7d = trx.select(date_sub(max(trx.dat_date_type_1),7)).alias("seven_days")
df1 = trx.select(add_months(max(trx.dat_date_type_1),-1)).alias("one_months")
df3 = trx.select(add_months(max(trx.dat_date_type_1),-3)).alias("three_months")
df6 = trx.select(add_months(max(trx.dat_date_type_1),-6)).alias("six_months")
df9 = trx.select(add_months(max(trx.dat_date_type_1),-9)).alias("nine_months")
df12 = trx.select(add_months(max(trx.dat_date_type_1),-12)).alias("twelve_month")
df24 = trx.select(add_months(max(trx.dat_date_type_1),-24)).alias("twentyfour_month")



df_max = trx.select(max(trx.dat_date_type_1)).alias("latest_months")


one_day_ago = df1d.collect()[0][0]
seven_days_ago = df7d.collect()[0][0]
latest_date = df_max.collect()[0][0]
current_month = latest_date.month
current_year = latest_date.year
one_month_ago = df1.collect()[0][0]
previous_month = one_month_ago.month
three_months_ago = df3.collect()[0][0]
six_months_ago = df6.collect()[0][0]
nine_months_ago = df9.collect()[0][0]
twelve_months_ago = df12.collect()[0][0]
previous_year = twelve_months_ago.year
twentyfour_months_ago = df24.collect()[0][0]

print("one_month_ago")
print(one_month_ago)
print("three_months_ago")
print(three_months_ago)
print("six_months_ago")
print(six_months_ago)
print("nine_months_ago")
print(nine_months_ago)
print("seven_days_ago")
print(seven_days_ago)
print("one_day_ago")
print(one_day_ago)


df_monetary = trx.groupBy(conformed.Transactions.idi_counterparty_gr.name).agg(sum(conformed.Transactions.cua_amount_type_1.name).alias("MONETARY"))

df_1m = trx.filter(F.col(conformed.Transactions.dat_date_type_1.name) >= one_month_ago).groupBy(conformed.Transactions.idi_counterparty_gr.name).agg(sum(conformed.Transactions.cua_amount_type_1.name).alias("MONETARY_1M"))
df_3m = trx.filter(F.col(conformed.Transactions.dat_date_type_1.name) >= three_months_ago).groupBy(conformed.Transactions.idi_counterparty_gr.name).agg(sum(conformed.Transactions.cua_amount_type_1.name).alias("MONETARY_3M"))
df_6m = trx.filter(F.col(conformed.Transactions.dat_date_type_1.name) >= six_months_ago).groupBy(conformed.Transactions.idi_counterparty_gr.name).agg(sum(conformed.Transactions.cua_amount_type_1.name).alias("MONETARY_6M"))
df_12m = trx.filter(F.col(conformed.Transactions.dat_date_type_1.name) >= twelve_months_ago).groupBy(conformed.Transactions.idi_counterparty_gr.name).agg(sum(conformed.Transactions.cua_amount_type_1.name).alias("MONETARY_12M"))
df_13_24m = trx.filter((F.col(conformed.Transactions.dat_date_type_1.name) >= twentyfour_months_ago) & (F.col(conformed.Transactions.dat_date_type_1.name) < twelve_months_ago)).groupBy(conformed.Transactions.idi_counterparty_gr.name).agg(sum(conformed.Transactions.cua_amount_type_1.name).alias("MONETARY_13_24M"))

df_agg_monetary = df_monetary\
.join(df_1m, [conformed.Transactions.idi_counterparty_gr.name], how="left")\
.join(df_3m, [conformed.Transactions.idi_counterparty_gr.name], how="left")\
.join(df_6m, [conformed.Transactions.idi_counterparty_gr.name], how="left")\
.join(df_12m, [conformed.Transactions.idi_counterparty_gr.name], how="left")\
.join(df_13_24m, [conformed.Transactions.idi_counterparty_gr.name], how="left")\


print('XXXXXXXXXXXXXXXXXXXX_df_agg_monetary', df_agg_monetary.printSchema())

################
################
################ rececny and other

columns_for_clv_monet_RECENCY = [
    conformed.Transactions.idi_counterparty_gr.name,
    conformed.Transactions.dat_date_type_1.name
]

columns_for_clv_input = [
    conformed.Transactions.idi_counterparty_gr.name
]

trx_Dept_desc = (Window.partitionBy(col(conformed.Transactions.idi_counterparty_gr.name))
                 .orderBy(col(conformed.Transactions.dat_date_type_1.name).desc())
                 )

trx_Dept_asc = (Window.partitionBy(col(conformed.Transactions.idi_counterparty_gr.name))
                .orderBy(col(conformed.Transactions.dat_date_type_1.name).asc())
                )

trx_LAST = (
    trx.select(*columns_for_clv_monet_RECENCY)
    .withColumn("row", row_number().over(trx_Dept_desc))
    .filter(col("row") == 1)
    .drop("row")
    .withColumn("LAST", col(conformed.Transactions.dat_date_type_1.name))
    .drop(col(conformed.Transactions.dat_date_type_1.name))
)

# trx_LAST_pd = trx_LAST.toPandas()

trx_FIRST = (
    trx.select(*columns_for_clv_monet_RECENCY)
    .withColumn("row", row_number().over(trx_Dept_asc))
    .filter(col("row") == 1)
    .drop("row")
    .withColumn("FIRST", col(conformed.Transactions.dat_date_type_1.name))
    .drop(col(conformed.Transactions.dat_date_type_1.name))
)

# trx_FIRST_pd = trx_LAST.toPandas()

trx_RECENCY = trx.select(*columns_for_clv_input)
trx_RECENCY = nullify_empty_cells(trx_RECENCY)
trx_RECENCY = (
    trx_RECENCY.dropDuplicates()
    .filter(col(conformed.Transactions.idi_counterparty_gr.name).isNotNull())
)

trx_RECENCY = (
    trx_RECENCY
    .join(trx_LAST, conformed.Transactions.idi_counterparty_gr.name, 'left')
    .join(trx_FIRST, conformed.Transactions.idi_counterparty_gr.name, 'left')
    .withColumn("RECENCY", datediff(col("LAST"), col("FIRST")))
    .drop(col("FIRST"))
    .drop(col("LAST"))
)

# Calculate Frequency Store Visit

df_freq_store = trx.groupBy(conformed.Transactions.idi_counterparty_gr.name).count().withColumnRenamed("count", "FREQUENCY_STOREVISIT")
df_last_dat_storevisit = trx.groupBy(conformed.Transactions.idi_counterparty_gr.name).agg(max(conformed.Transactions.dat_date_type_1.name).alias("LAST_DAT_STOREVISIT"))

df_1m = trx.filter(F.col(conformed.Transactions.dat_date_type_1.name) >= one_month_ago).groupBy(conformed.Transactions.idi_counterparty_gr.name).count().withColumnRenamed("count", "FREQUENCY_STOREVISIT_1M")
df_3m = trx.filter(F.col(conformed.Transactions.dat_date_type_1.name) >= three_months_ago).groupBy(conformed.Transactions.idi_counterparty_gr.name).count().withColumnRenamed("count", "FREQUENCY_STOREVISIT_3M")
df_6m = trx.filter(F.col(conformed.Transactions.dat_date_type_1.name) >= six_months_ago).groupBy(conformed.Transactions.idi_counterparty_gr.name).count().withColumnRenamed("count", "FREQUENCY_STOREVISIT_6M")
df_12m = trx.filter(F.col(conformed.Transactions.dat_date_type_1.name) >= twelve_months_ago).groupBy(conformed.Transactions.idi_counterparty_gr.name).count().withColumnRenamed("count", "FREQUENCY_STOREVISIT_12M")


df_agg_freq_store = df_freq_store\
.join(df_1m, [conformed.Transactions.idi_counterparty_gr.name], how="left")\
.join(df_3m, [conformed.Transactions.idi_counterparty_gr.name], how="left")\
.join(df_6m, [conformed.Transactions.idi_counterparty_gr.name], how="left")\
.join(df_12m, [conformed.Transactions.idi_counterparty_gr.name], how="left")\
.join(df_last_dat_storevisit, [conformed.Transactions.idi_counterparty_gr.name], how="left")


print('XXXXXXXXXXXXXXXXXXXX_df_agg_freq_store', df_agg_freq_store.printSchema())

frequency_absolute_df = df_agg_freq_store.withColumn("BUYING_CYCLE", (F.lit(90)/(F.col("FREQUENCY_STOREVISIT_3M") - F.lit(1))))
churn_df = frequency_absolute_df.join(trx_RECENCY, conformed.Transactions.idi_counterparty_gr.name, how="outer")
churn_df = churn_df.withColumn("VISIT_STATE", (F.col("RECENCY")/F.col("BUYING_CYCLE")))

churn_df = churn_df.withColumn(
    "CHURN_STATUS",
    when((churn_df.VISIT_STATE > 1) & (churn_df.VISIT_STATE <=2),  "WARNING").when(churn_df.VISIT_STATE > 2,  "AT RISK").otherwise("OK")
)

churn_df.show(20)

print('XXXXXXXXXXXXXXXXXXXX_churn_df', churn_df.printSchema())



gcr_joined_trx_FRTM_2 = df_agg_monetary.select(conformed.Transactions.idi_counterparty_gr.name,"MONETARY_3M","MONETARY_6M","MONETARY_12M")

### TURNOVER_3M_NI

trx_ni = DeltaTable.forPath(spark, conformed_trans_path).toDF().filter(col(conformed.Transactions.bu.name) == 'ni').filter(col(conformed.Transactions.dat_date_type_1.name) >= '2022-01-02')
trx_MONET_3M_ni = trx_ni.filter(F.col(conformed.Transactions.dat_date_type_1.name) >= three_months_ago).groupBy(conformed.Transactions.idi_counterparty_gr.name).agg(sum(conformed.Transactions.cua_amount_type_1.name).alias("MONETARY_3M_NI"))
gcr_joined_trx_FRTM_2 = gcr_joined_trx_FRTM_2.join(trx_MONET_3M_ni, conformed.Transactions.idi_counterparty_gr.name, 'left')

### TURNOVER_3M_MOE

trx_ni = DeltaTable.forPath(spark, conformed_trans_path).toDF().filter(col(conformed.Transactions.des_store_name.name) == 'Mall of the Emirates').filter(col(conformed.Transactions.dat_date_type_1.name) >= '2022-01-02')
trx_MONET_3M_ni = trx_ni.filter(F.col(conformed.Transactions.dat_date_type_1.name) >= three_months_ago).groupBy(conformed.Transactions.idi_counterparty_gr.name).agg(sum(conformed.Transactions.cua_amount_type_1.name).alias("MONETARY_3M_MOE"))
gcr_joined_trx_FRTM_2 = gcr_joined_trx_FRTM_2.join(trx_MONET_3M_ni, conformed.Transactions.idi_counterparty_gr.name, 'left')


### TURNOVER_3M_CC_D

trx_ni = DeltaTable.forPath(spark, conformed_trans_path).toDF().filter(col(conformed.Transactions.des_store_name.name) == 'City Centre Deira').filter(col(conformed.Transactions.dat_date_type_1.name) >= '2022-01-02')
trx_MONET_3M_ni = trx_ni.filter(F.col(conformed.Transactions.dat_date_type_1.name) >= three_months_ago).groupBy(conformed.Transactions.idi_counterparty_gr.name).agg(sum(conformed.Transactions.cua_amount_type_1.name).alias("MONETARY_3M_CC_D"))
gcr_joined_trx_FRTM_2 = gcr_joined_trx_FRTM_2.join(trx_MONET_3M_ni, conformed.Transactions.idi_counterparty_gr.name, 'left')

# ni pta_sta
columns_for_pta_sta = [
    conformed.PtaSta.idi_counterparty_gr.name,
    conformed.PtaSta.pta.name,
    conformed.PtaSta.sta.name,
]

pta_sta = DeltaTable.forPath(spark, get_s3_path(conformed.PtaSta, lake_descriptor)).toDF()
pta_sta = pta_sta.select(*columns_for_pta_sta)

gcr_joined_trx_FRTM_2 = gcr_joined_trx_FRTM_2.join(pta_sta, conformed.Transactions.idi_counterparty_gr.name, 'left')


print('XXXXXXXXXXXXXXXXXXXXpta_sta', gcr_joined_trx_FRTM_2.printSchema())


# ni NI_LOCATIONS

columns_for_ni_location = [
    conformed.Transactions.idi_counterparty_gr.name,
    conformed.Transactions.des_mall_name.name
]
trx_ni = DeltaTable.forPath(spark, conformed_trans_path).toDF().filter(col(conformed.Transactions.bu.name) == 'ni').filter(col(conformed.Transactions.dat_date_type_1.name) >= '2022-01-02')
trx_ni_location = trx_ni.select(*columns_for_ni_location).dropDuplicates(columns_for_ni_location).groupBy(conformed.Transactions.idi_counterparty_gr.name).agg(func.concat_ws(" ", func.collect_list(conformed.Transactions.des_mall_name.name)).alias("NI_LOCATIONS"))
gcr_joined_trx_FRTM_2 = gcr_joined_trx_FRTM_2.join(trx_ni_location, conformed.Transactions.idi_counterparty_gr.name, 'left')


# ni BU_TURNOVER

columns_for_bu_turnover = [
    conformed.Transactions.idi_counterparty_gr.name,
    conformed.Transactions.cde_base_bu_key.name
]

#
bu_dict = DeltaTable.forPath(spark, get_s3_path(conformed.LoyaltyBusinessUnit, lake_descriptor)).toDF()
trx_ni_location = trx.select(*columns_for_bu_turnover)

trx_ni_location = (
    trx_ni_location.
    join(bu_dict, on=bu_dict.bu_key == trx_ni_location.cde_base_bu_key, how='left')
    .select([
        col(conformed.Transactions.idi_counterparty_gr.name),
        col("code"),
    ])
    .withColumnRenamed("code", conformed.Transactions.cde_base_bu_key.name)
)

trx_ni_location = trx_ni_location.dropDuplicates(columns_for_bu_turnover).groupBy(conformed.Transactions.idi_counterparty_gr.name).agg(func.concat_ws(" ", func.collect_list(conformed.Transactions.cde_base_bu_key.name)).alias("BU_TURNOVER"))
gcr_joined_trx_FRTM_2 = gcr_joined_trx_FRTM_2.join(trx_ni_location, conformed.Transactions.idi_counterparty_gr.name, 'left')

#category mapping
trx_ni = (
    DeltaTable.forPath(spark, conformed_trans_path).toDF()
    .filter(col(conformed.Transactions.bu.name) == "share")
    .filter(col(conformed.Transactions.dat_date_type_1.name) >= '2022-01-02')
)
#experiences
trx_ni_location = trx_ni.dropDuplicates(["idi_counterparty_gr","des_store_experience"]).groupBy(conformed.Transactions.idi_counterparty_gr.name).agg(func.concat_ws(" ", func.collect_list(conformed.Transactions.des_store_experience.name)).alias("experiences"))
gcr_joined_trx_FRTM_2 = gcr_joined_trx_FRTM_2.join(trx_ni_location, conformed.Transactions.idi_counterparty_gr.name, 'left')
#category_group
trx_ni_location = trx_ni.dropDuplicates(["idi_counterparty_gr","des_category_group"]).groupBy(conformed.Transactions.idi_counterparty_gr.name).agg(func.concat_ws(" ", func.collect_list(conformed.Transactions.des_category_group.name)).alias("category_group"))
gcr_joined_trx_FRTM_2 = gcr_joined_trx_FRTM_2.join(trx_ni_location, conformed.Transactions.idi_counterparty_gr.name, 'left')
#store_category
trx_ni_location = trx_ni.dropDuplicates(["idi_counterparty_gr","des_store_category"]).groupBy(conformed.Transactions.idi_counterparty_gr.name).agg(func.concat_ws(" ", func.collect_list(conformed.Transactions.des_store_category.name)).alias("store_category"))
gcr_joined_trx_FRTM_2 = gcr_joined_trx_FRTM_2.join(trx_ni_location, conformed.Transactions.idi_counterparty_gr.name, 'left')


# st_mall & st_brand
location = DeltaTable.forPath(spark, get_s3_path(conformed.Location, lake_descriptor)).toDF()

trx_ni = (
    DeltaTable.forPath(spark, conformed_trans_path).toDF()
    .filter(col(conformed.Transactions.bu.name) == "share")
    .filter(col(conformed.Transactions.dat_date_type_1.name) >= '2022-01-02')
    .select(["idi_counterparty_gr","cde_store_key"])
    .join(location, on= conformed.Location.cde_store_key.name, how='left')
)


#st_mall

trx_ni_location = trx_ni.dropDuplicates(["idi_counterparty_gr","des_mall_name"]).groupBy(conformed.Transactions.idi_counterparty_gr.name).agg(func.concat_ws(" ", func.collect_list(conformed.Location.des_mall_name.name)).alias("st_mall"))
gcr_joined_trx_FRTM_2 = gcr_joined_trx_FRTM_2.join(trx_ni_location, conformed.Transactions.idi_counterparty_gr.name, 'left')


#st_brand

trx_ni_location = trx_ni.dropDuplicates(["idi_counterparty_gr","des_maf_brand_name"]).groupBy(conformed.Transactions.idi_counterparty_gr.name).agg(func.concat_ws(" ", func.collect_list(conformed.Location.des_maf_brand_name.name)).alias("st_brand"))
gcr_joined_trx_FRTM_2 = gcr_joined_trx_FRTM_2.join(trx_ni_location, conformed.Transactions.idi_counterparty_gr.name, 'left')


print('XXXXXXXXXXXXXXXXXXXX_trx_ni_location', trx_ni_location.printSchema())


# ,"FREQUENCY_STOREVISIT_12M","CHURN_STATUS"

churn_df_selected = churn_df.select(conformed.Transactions.idi_counterparty_gr.name,"FREQUENCY_STOREVISIT_12M","CHURN_STATUS")
gcr_joined_trx_FRTM_2 = gcr_joined_trx_FRTM_2.join(churn_df_selected, conformed.Transactions.idi_counterparty_gr.name, 'left')


churn_df_selected.show(20)
# SOW
gcr_joined_trx_FRTM_2 = gcr_joined_trx_FRTM_2.withColumn("SOW", (F.col("MONETARY_3M") / F.col("MONETARY_3M_NI")* 100 ))

# SOW_MOE
gcr_joined_trx_FRTM_2 = gcr_joined_trx_FRTM_2.withColumn("SOW_MOE", (F.col("MONETARY_3M") / F.col("MONETARY_3M_MOE")* 100 ))

# SOW_CC_D
gcr_joined_trx_FRTM_2 = gcr_joined_trx_FRTM_2.withColumn("SOW_CC_D", (F.col("MONETARY_3M") / F.col("MONETARY_3M_CC_D")* 100 ))


gcr_joined_trx_FRTM_2 = gcr_joined_trx_FRTM_2.withColumn("SOW", F.round(gcr_joined_trx_FRTM_2["SOW"]).cast('integer'))
gcr_joined_trx_FRTM_2 = gcr_joined_trx_FRTM_2.withColumn("SOW_MOE", F.round(gcr_joined_trx_FRTM_2["SOW_MOE"]).cast('integer'))
gcr_joined_trx_FRTM_2 = gcr_joined_trx_FRTM_2.withColumn("SOW_CC_D",  F.round(gcr_joined_trx_FRTM_2["SOW_CC_D"]).cast('integer'))
gcr_joined_trx_FRTM_2 = gcr_joined_trx_FRTM_2.withColumn("PTA", col("PTA").cast('string'))
gcr_joined_trx_FRTM_2 = gcr_joined_trx_FRTM_2.withColumn("STA", col("STA").cast('string'))
gcr_joined_trx_FRTM_2 = gcr_joined_trx_FRTM_2.withColumn("TURNOVER_3M", F.round(gcr_joined_trx_FRTM_2["MONETARY_3M"]).cast('integer'))
gcr_joined_trx_FRTM_2 = gcr_joined_trx_FRTM_2.withColumn("TURNOVER_6M", F.round(gcr_joined_trx_FRTM_2["MONETARY_6M"]).cast('integer'))
gcr_joined_trx_FRTM_2 = gcr_joined_trx_FRTM_2.withColumn("TURNOVER_12M", F.round(gcr_joined_trx_FRTM_2["MONETARY_12M"]).cast('integer'))
gcr_joined_trx_FRTM_2 = gcr_joined_trx_FRTM_2.withColumn("TURNOVER_3M_MOE", lit(None).cast('integer'))
gcr_joined_trx_FRTM_2 = gcr_joined_trx_FRTM_2.withColumn("TURNOVER_3M_NI", F.round(gcr_joined_trx_FRTM_2["MONETARY_3M_NI"]).cast('integer'))
gcr_joined_trx_FRTM_2 = gcr_joined_trx_FRTM_2.withColumn("ENGAGEMENT_STATUS", col("CHURN_STATUS").cast('string'))
gcr_joined_trx_FRTM_2 = gcr_joined_trx_FRTM_2.withColumn("BU_TURNOVER", col("BU_TURNOVER").cast('string'))
gcr_joined_trx_FRTM_2 = gcr_joined_trx_FRTM_2.withColumn("NI_LOCATIONS", col("NI_LOCATIONS").cast('string'))
gcr_joined_trx_FRTM_2 = gcr_joined_trx_FRTM_2.withColumn("NUMBER_OF_VISITS_12M", col("FREQUENCY_STOREVISIT_12M").cast('integer'))


gcr_joined_trx_FRTM_2 = gcr_joined_trx_FRTM_2.drop("MONETARY_3M","MONETARY_6M","MONETARY_12M","MONETARY_3M_NI","FREQUENCY_STOREVISIT_12M","CHURN_STATUS","MONETARY_3M_CC_D","MONETARY_3M_MOE")


gcr_joined_trx_FRTM_2 = monteary_brand_add('share', "12", three_months_ago, "total_spend_als_last_3_months", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = monteary_brand_add('share', "12", six_months_ago, "total_spend_als_last_6_months", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = monteary_brand_add('share', "2", three_months_ago, "total_spend_carrefour_last_3_months", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = monteary_brand_add('share', "2", six_months_ago, "total_spend_carrefour_last_6_months", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = monteary_mall_add('share', "City Centre Deira", three_months_ago, "total_spend_ccd_last_3_months", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = monteary_mall_add('share', "City Centre Deira", six_months_ago, "total_spend_ccd_last_6_months", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = monteary_mall_add('share', "City Centre Mirdif", three_months_ago, "total_spend_ccmi_last_3_months", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = monteary_mall_add('share', "City Centre Mirdif", six_months_ago, "total_spend_ccmi_last_6_months", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = monteary_brand_add('share', "14", three_months_ago, "total_spend_cnb_last_3_months", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = monteary_brand_add('share', "14", six_months_ago, "total_spend_cnb_last_6_months", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = monteary_brand_add('share', "2", three_months_ago,  "total_spend_crf_last_3_months", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = monteary_brand_add('share', "2", six_months_ago,    "total_spend_crf_last_6_months", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = monteary_custom_add('share', "lec", three_months_ago, "total_spend_lec_last_3_months", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = monteary_custom_add('share', "lec", six_months_ago, "total_spend_lec_last_6_months", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = monteary_custom_add('share', "lifestyle", three_months_ago, "total_spend_lifestyle_last_3_months", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = monteary_custom_add('share', "lifestyle", six_months_ago, "total_spend_lifestyle_last_6_months", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = monteary_brand_add('share', "13", three_months_ago, "total_spend_lll_last_3_months", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = monteary_brand_add('share', "13", six_months_ago, "total_spend_lll_last_6_months", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = monteary_brand_add('share', "3", three_months_ago, "total_spend_magicp_last_3_months", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = monteary_mall_add('share', "Mall of the Emirates", twelve_months_ago, "total_spend_moe_last_12_months", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = monteary_mall_add('share', "Mall of the Emirates", one_day_ago, "total_spend_moe_last_1_day", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = monteary_mall_add('share', "Mall of the Emirates", one_month_ago, "total_spend_moe_last_1_month", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = monteary_mall_add('share', "Mall of the Emirates", three_months_ago, "total_spend_moe_last_3_months", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = monteary_mall_add('share', "Mall of the Emirates", six_months_ago, "total_spend_moe_last_6_months", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = monteary_mall_add('share', "Mall of the Emirates", seven_days_ago, "total_spend_moe_last_7_days", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = monteary_bu_add("share", three_months_ago, "total_spend_share_last_3_months", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = monteary_bu_add("share", six_months_ago, "total_spend_share_last_6_months", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = monteary_bu_add("share", seven_days_ago, "total_spend_share_last_7_days", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = monteary_brand_add('share', "35", three_months_ago, "total_spend_skidub_last_3_months", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = monteary_brand_add('share', "35", six_months_ago, "total_spend_skidub_last_6_months", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = monteary_custom_add('share', "smbu", three_months_ago, "total_spend_smbu_last_3_months", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = monteary_custom_add('share', "smbu", six_months_ago, "total_spend_smbu_last_6_months", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = monteary_brand_add('share', "4", three_months_ago, "total_spend_vox_last_3_months", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = monteary_brand_add('share', "4", six_months_ago, "total_spend_vox_last_6_months", gcr_joined_trx_FRTM_2)

# partner data

gcr_joined_trx_FRTM_2 = total_transactions_custom_add('share', "partner", one_day_ago, "total_transactions_share_partner_last_1_day", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = total_transactions_custom_add('share', "partner", seven_days_ago, "total_transactions_share_partner_last_7_days", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = total_transactions_custom_add('share', "partner", one_month_ago, "total_transactions_share_partner_last_1_month", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = total_transactions_custom_add('share', "partner", three_months_ago, "total_transactions_share_partner_last_3_months", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = total_transactions_custom_add('share', "partner", six_months_ago, "total_transactions_share_partner_last_6_months", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = total_transactions_custom_add('share', "partner", twelve_months_ago, "total_transactions_share_partner_last_12_months", gcr_joined_trx_FRTM_2)

gcr_joined_trx_FRTM_2 = monteary_custom_add('share', "partner", one_day_ago, "total_spend_share_partner_last_1_day", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = monteary_custom_add('share', "partner", seven_days_ago, "total_spend_share_partner_last_7_days", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = monteary_custom_add('share', "partner", one_month_ago, "total_spend_share_partner_last_1_month", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = monteary_custom_add('share', "partner", three_months_ago, "total_spend_share_partner_last_3_months", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = monteary_custom_add('share', "partner", six_months_ago, "total_spend_share_partner_last_6_months", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = monteary_custom_add('share', "partner", twelve_months_ago, "total_spend_share_partner_last_12_months", gcr_joined_trx_FRTM_2)

gcr_joined_trx_FRTM_2 = find_first_last_trx_date_custom_add("partner", "first", "partner_first_transaction_date", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = find_first_last_trx_date_custom_add("partner", "last", "partner_last_transaction_date", gcr_joined_trx_FRTM_2)

gcr_joined_trx_FRTM_2 = engagement_status_custom_add("partner", "partner_engagement_status", gcr_joined_trx_FRTM_2, six_months_ago )

# Category_Mapping


gcr_joined_trx_FRTM_2 = monteary_custom_add('share', "Mall of the Emirates", seven_days_ago, "total_spend_moe_luxury_last_7_days", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = monteary_custom_add('share', "Mall of the Emirates", one_month_ago, "total_spend_moe_luxury_last_1_month", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = monteary_custom_add('share', "Mall of the Emirates", three_months_ago, "total_spend_moe_luxury_last_3_months", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = monteary_custom_add('share', "Mall of the Emirates", six_months_ago, "total_spend_moe_luxury_last_6_months", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = monteary_custom_add('share', "Mall of the Emirates", twelve_months_ago, "total_spend_moe_luxury_last_12_months", gcr_joined_trx_FRTM_2)


gcr_joined_trx_FRTM_2 = monteary_custom_add('share', "City Centre Mirdif", seven_days_ago, "total_spend_ccmi_luxury_last_7_days", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = monteary_custom_add('share', "City Centre Mirdif", one_month_ago, "total_spend_ccmi_luxury_last_1_month", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = monteary_custom_add('share', "City Centre Mirdif", three_months_ago, "total_spend_ccmi_luxury_last_3_months", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = monteary_custom_add('share', "City Centre Mirdif", six_months_ago, "total_spend_ccmi_luxury_last_6_months", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = monteary_custom_add('share', "City Centre Mirdif", twelve_months_ago, "total_spend_ccmi_luxury_last_12_months", gcr_joined_trx_FRTM_2)

# Partner Data Identification 2.0

gcr_joined_trx_FRTM_2 = total_transactions_custom_add('share', "94", one_day_ago, "total_transactions_share_partner_fab_last_1_day", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = total_transactions_custom_add('share', "94", seven_days_ago, "total_transactions_share_partner_fab_last_7_day", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = total_transactions_custom_add('share', "94", one_month_ago, "total_transactions_share_partner_fab_last_1_months", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = total_transactions_custom_add('share', "94", three_months_ago, "total_transactions_share_partner_fab_last_3_months", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = total_transactions_custom_add('share', "94", six_months_ago, "total_transactions_share_partner_fab_last_6_months", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = total_transactions_custom_add('share', "94", twelve_months_ago, "total_transactions_share_partner_fab_last_12_months", gcr_joined_trx_FRTM_2)

gcr_joined_trx_FRTM_2 = monteary_brand_add('share', "94", one_day_ago, "total_spend_share_fab_last_1_day", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = monteary_brand_add('share', "94", seven_days_ago, "total_spend_share_fab_last_7_day", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = monteary_brand_add('share', "94", one_month_ago, "total_spend_share_fab_last_1_month", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = monteary_brand_add('share', "94", three_months_ago, "total_spend_share_fab_last_3_month", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = monteary_brand_add('share', "94", six_months_ago, "total_spend_share_fab_last_6_month", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = monteary_brand_add('share', "94", twelve_months_ago, "total_spend_share_fab_last_12_month", gcr_joined_trx_FRTM_2)

gcr_joined_trx_FRTM_2 = find_first_last_trx_date_custom_add("94", "first", "share_partner_fab_first_transaction_date", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = find_first_last_trx_date_custom_add("94", "last", "share_partner_fab_last_transaction_date", gcr_joined_trx_FRTM_2)

gcr_joined_trx_FRTM_2 = engagement_status_custom_add('share', "94", "share_partner_fab_engagement_status", gcr_joined_trx_FRTM_2, six_months_ago )
gcr_joined_trx_FRTM_2 = engagement_status_custom_add('share', "78", "share_partner_costa_engagement_status", gcr_joined_trx_FRTM_2, six_months_ago)
gcr_joined_trx_FRTM_2 = engagement_status_custom_add('share', "79", "share_partner_gn_engagement_status", gcr_joined_trx_FRTM_2, six_months_ago)
gcr_joined_trx_FRTM_2 = engagement_status_custom_add('share', "72", "share_partner_etihad_engagement_status", gcr_joined_trx_FRTM_2, six_months_ago)
gcr_joined_trx_FRTM_2 = engagement_status_custom_add('share', "98", "share_partner_aljaber_engagement_status", gcr_joined_trx_FRTM_2, six_months_ago)
gcr_joined_trx_FRTM_2 = engagement_status_custom_add('share', "101", "share_partner_smiles_engagement_status", gcr_joined_trx_FRTM_2, six_months_ago)
gcr_joined_trx_FRTM_2 = engagement_status_custom_add('share', "92", "share_partner_bookingcom_engagement_status", gcr_joined_trx_FRTM_2, six_months_ago)


#### LIFESTYLE


gcr_joined_trx_FRTM_2 = engagement_status_custom_add('fsn_new', "THAT", "lifestyle_that_engagement_status_6_months", gcr_joined_trx_FRTM_2, six_months_ago)
gcr_joined_trx_FRTM_2 = engagement_status_custom_add('fsn_new', "LuluLemon (Store)", "lifestyle_lll_engagement_status_6_months", gcr_joined_trx_FRTM_2, six_months_ago)
gcr_joined_trx_FRTM_2 = engagement_status_custom_add('fsn_new', "All Saints (Store)", "lifestyle_als_engagement_status_6_months", gcr_joined_trx_FRTM_2, six_months_ago)
gcr_joined_trx_FRTM_2 = engagement_status_custom_add('fsn_new', "LEGO (Store)", "lifestyle_lego_engagement_status_6_months", gcr_joined_trx_FRTM_2, six_months_ago)
gcr_joined_trx_FRTM_2 = engagement_status_custom_add('fsn_new', "Shiseido (Store)", "lifestyle_shi_engagement_status_6_months", gcr_joined_trx_FRTM_2, six_months_ago)
gcr_joined_trx_FRTM_2 = engagement_status_custom_add('fsn_new', "Crate & Barrel (Store)", "lifestyle_cnb_engagement_status_9_months", gcr_joined_trx_FRTM_2, nine_months_ago)
gcr_joined_trx_FRTM_2 = engagement_status_custom_add('fsn_new', "CB2 (Store)", "lifestyle_cb2_engagement_status_9_months", gcr_joined_trx_FRTM_2, nine_months_ago)


gcr_joined_trx_FRTM_2 = total_transactions_custom_add('fsn_new', "THAT", twentyfour_months_ago, "total_transactions_lifestyle_that", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = total_transactions_custom_add('fsn_new', "LuluLemon (Store)", twentyfour_months_ago, "total_transactions_lifestyle_lll", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = total_transactions_custom_add('fsn_new', "All Saints (Store)", twentyfour_months_ago, "total_transactions_lifestyle_als", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = total_transactions_custom_add('fsn_new', "LEGO (Store)", twentyfour_months_ago, "total_transactions_lifestyle_lego", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = total_transactions_custom_add('fsn_new', "Shiseido (Store)", twentyfour_months_ago, "total_transactions_lifestyle_shi", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = total_transactions_custom_add('fsn_new', "Crate & Barrel (Store)", twentyfour_months_ago, "total_transactions_lifestyle_cnb", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = total_transactions_custom_add('fsn_new', "CB2 (Store)", twentyfour_months_ago, "total_transactions_lifestyle_cb2", gcr_joined_trx_FRTM_2)

gcr_joined_trx_FRTM_2 = monteary_lifestyle_brand_add("THAT", six_months_ago, "total_spend_lifestyle_that_6_months", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = monteary_lifestyle_brand_add("LuluLemon (Store)", six_months_ago, "total_spend_lifestyle_lll_6_months", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = monteary_lifestyle_brand_add("All Saints (Store)", six_months_ago, "total_spend_lifestyle_als_6_months", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = monteary_lifestyle_brand_add("LEGO (Store)", six_months_ago, "total_spend_lifestyle_lego_6_months", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = monteary_lifestyle_brand_add("Shiseido (Store)", six_months_ago, "total_spend_lifestyle_shi_6_months", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = monteary_lifestyle_brand_add("Crate & Barrel (Store)", six_months_ago, "total_spend_lifestyle_cnb_6_months", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = monteary_lifestyle_brand_add("CB2 (Store)", six_months_ago, "total_spend_lifestyle_cb2_6_months", gcr_joined_trx_FRTM_2)


gcr_joined_trx_FRTM_2 = average_customer_spend_brand_add('fsn_new',"THAT", six_months_ago, "average_customer_spend_lifestyle_that_last_6_months", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = average_customer_spend_brand_add('fsn_new',"LuluLemon (Store)", six_months_ago, "average_customer_spend_lifestyle_lll_last_6_months", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = average_customer_spend_brand_add('fsn_new',"All Saints (Store)", six_months_ago, "average_customer_spend_lifestyle_als_last_6_months", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = average_customer_spend_brand_add('fsn_new',"LEGO (Store)", six_months_ago, "average_customer_spend_lifestyle_lego_last_6_months", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = average_customer_spend_brand_add('fsn_new',"Shiseido (Store)", six_months_ago, "average_customer_spend_lifestyle_shi_last_6_months", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = average_customer_spend_brand_add('fsn_new',"Crate & Barrel (Store)", six_months_ago, "average_customer_spend_lifestyle_cnb_last_6_months", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = average_customer_spend_brand_add('fsn_new',"CB2 (Store)", six_months_ago, "average_customer_spend_lifestyle_cb2_last_6_months", gcr_joined_trx_FRTM_2)

gcr_joined_trx_FRTM_2 = average_customer_spend_brand_add('fsn_new',"THAT", nine_months_ago, "average_customer_spend_lifestyle_that_last_9_months", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = average_customer_spend_brand_add('fsn_new',"LuluLemon (Store)", nine_months_ago, "average_customer_spend_lifestyle_lll_last_9_months", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = average_customer_spend_brand_add('fsn_new',"All Saints (Store)", nine_months_ago, "average_customer_spend_lifestyle_als_last_9_months", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = average_customer_spend_brand_add('fsn_new',"LEGO (Store)", nine_months_ago, "average_customer_spend_lifestyle_lego_last_9_months", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = average_customer_spend_brand_add('fsn_new',"Shiseido (Store)", nine_months_ago, "average_customer_spend_lifestyle_shi_last_9_months", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = average_customer_spend_brand_add('fsn_new',"Crate & Barrel (Store)", nine_months_ago, "average_customer_spend_lifestyle_cnb_last_9_months", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = average_customer_spend_brand_add('fsn_new',"CB2 (Store)", nine_months_ago, "average_customer_spend_lifestyle_cb2_last_9_months", gcr_joined_trx_FRTM_2)

gcr_joined_trx_FRTM_2 = average_customer_spend_brand_add('fsn_new',"THAT", twelve_months_ago, "average_customer_spend_lifestyle_that_last_12_months", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = average_customer_spend_brand_add('fsn_new',"LuluLemon (Store)", twelve_months_ago, "average_customer_spend_lifestyle_lll_last_12_months", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = average_customer_spend_brand_add('fsn_new',"All Saints (Store)", twelve_months_ago, "average_customer_spend_lifestyle_als_last_12_months", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = average_customer_spend_brand_add('fsn_new',"LEGO (Store)", twelve_months_ago, "average_customer_spend_lifestyle_lego_last_12_months", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = average_customer_spend_brand_add('fsn_new',"Shiseido (Store)", twelve_months_ago, "average_customer_spend_lifestyle_shi_last_12_months", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = average_customer_spend_brand_add('fsn_new',"Crate & Barrel (Store)", twelve_months_ago, "average_customer_spend_lifestyle_cnb_last_12_months", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = average_customer_spend_brand_add('fsn_new',"CB2 (Store)", twelve_months_ago, "average_customer_spend_lifestyle_cb2_last_12_months", gcr_joined_trx_FRTM_2)

gcr_joined_trx_FRTM_2 = average_customer_spend_brand_add('fsn_new',"THAT", twentyfour_months_ago, "avg_transaction_value_lifestyle_that", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = average_customer_spend_brand_add('fsn_new',"LuluLemon (Store)", twentyfour_months_ago, "avg_transaction_value_lifestyle_lll", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = average_customer_spend_brand_add('fsn_new',"All Saints (Store)", twentyfour_months_ago, "avg_transaction_value_lifestyle_als", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = average_customer_spend_brand_add('fsn_new',"LEGO (Store)", twentyfour_months_ago, "avg_transaction_value_lifestyle_lego", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = average_customer_spend_brand_add('fsn_new',"Shiseido (Store)", twentyfour_months_ago, "avg_transaction_value_lifestyle_shi", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = average_customer_spend_brand_add('fsn_new',"Crate & Barrel (Store)", twentyfour_months_ago, "avg_transaction_value_lifestyle_cnb", gcr_joined_trx_FRTM_2)
gcr_joined_trx_FRTM_2 = average_customer_spend_brand_add('fsn_new',"CB2 (Store)", twentyfour_months_ago, "avg_transaction_value_lifestyle_cb2", gcr_joined_trx_FRTM_2)

gcr_joined_trx_FRTM_2 = engagement_status_custom_inactive_add('fsn_new', "THAT", "lifestyle_that_engagement_status_6_months", gcr_joined_trx_FRTM_2, six_months_ago)
gcr_joined_trx_FRTM_2 = engagement_status_custom_inactive_add('fsn_new', "LuluLemon (Store)", "lifestyle_lll_engagement_status_6_months", gcr_joined_trx_FRTM_2, six_months_ago)
gcr_joined_trx_FRTM_2 = engagement_status_custom_inactive_add('fsn_new', "All Saints (Store)", "lifestyle_als_engagement_status_6_months", gcr_joined_trx_FRTM_2, six_months_ago)
gcr_joined_trx_FRTM_2 = engagement_status_custom_inactive_add('fsn_new', "LEGO (Store)", "lifestyle_lego_engagement_status_6_months", gcr_joined_trx_FRTM_2, six_months_ago)
gcr_joined_trx_FRTM_2 = engagement_status_custom_inactive_add('fsn_new', "Shiseido (Store)", "lifestyle_shi_engagement_status_6_months", gcr_joined_trx_FRTM_2, six_months_ago)
gcr_joined_trx_FRTM_2 = engagement_status_custom_inactive_add('fsn_new', "Crate & Barrel (Store)", "lifestyle_cnb_engagement_status_9_months", gcr_joined_trx_FRTM_2, nine_months_ago)
gcr_joined_trx_FRTM_2 = engagement_status_custom_inactive_add('fsn_new', "CB2 (Store)", "lifestyle_cb2_engagement_status_9_months", gcr_joined_trx_FRTM_2, nine_months_ago)

print('XXXXXXXXXXXXXXXXXXXX_end', gcr_joined_trx_FRTM_2.printSchema())

creat_delta_table_if_not_exists(spark, conformed.ProfileCalculatedKpi, lake_descriptor)

kpi_path = get_s3_path(conformed.ProfileCalculatedKpi, lake_descriptor)

gcr_joined_trx_FRTM_2.write.format('delta').mode('overwrite').save(kpi_path)

#gcr_joined_trx_FRTM_2.write.mode('overwrite').parquet(kpi_path)


#
# lifestyle_that_engagement_status_6_months
# lifestyle_lll_engagement_status_6_months
# lifestyle_als_engagement_status_6_months
# lifestyle_lego_engagement_status_6_months
# lifestyle_shi_engagement_status_6_months
# lifestyle_cnb_engagement_status_9_months
# lifestyle_cb2_engagement_status_9_months