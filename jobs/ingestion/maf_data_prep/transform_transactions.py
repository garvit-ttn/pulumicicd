from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import *

# from delta import DeltaTable

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

spark = glueContext.spark_session

trx_headers = spark.read.csv(
    path="s3://cvm-develop/transactions/share/share_transactions_header/2022-09-12/part-00000-773e379e-38e8-4d57-a314-9c7c3c5fe29d-c000.csv",
    sep='|',
    inferSchema=True,
    header=True
)

trx_items = spark.read.csv(
    path="s3://cvm-develop/transactions/share/share_transactions_items/2022-09-12/part-00000-ab29e488-6840-4446-8792-1ea301d7df98-c000.csv",
    sep='|',
    inferSchema=True,
    header=True
)

trx_full = trx_items.join(trx_headers, 'trx_key', how='left')

trx_full = trx_full \
    .withColumnRenamed('trx_key', 'IDI_TURNOVER') \
    .withColumnRenamed('customer_id', 'IDI_COUNTERPARTY') \
    .withColumnRenamed('txn_value', 'CUA_AMOUNT_TYPE_1') \
    .withColumnRenamed('total_sale_amount', 'CUA_AMOUNT_TYPE_2') \
    .withColumnRenamed('currency_key', 'CDE_CURRENCY') \
    .withColumnRenamed('txn_date', 'DAT_DATE_TYPE_1') \
    .withColumnRenamed('(default 1)', 'QTY_NUMBER_OF_ITEMS') \
    .withColumnRenamed('product_id', 'IDI_PROPOSITION') \
    .withColumnRenamed('txn_line_items', 'NAM_PROPOSITION') \
    .withColumnRenamed('date_key', 'CDE_DATE_KEY') \
    .withColumnRenamed('store_key', 'CDE_STORE_KEY') \
    .withColumnRenamed('country_key', 'CDE_COUNTRY_KEY') \
    .withColumnRenamed('country', 'DES_COUNTRY_NAME') \
    .withColumnRenamed('ds.store_name as store', 'DES_STORE_NAME') \
    .withColumnRenamed('card_number', 'DES_OFLINE_ONLINE') \
    .withColumnRenamed('online_offline', 'CDE_TRX_KEY') \
    .withColumnRenamed('txn_location', 'DES_CITYNAME') \
    .withColumnRenamed('visit_key', 'CDE_VISIT_KEY') \
    .withColumnRenamed('store_code', 'CDE_STORE_CODE') \
    .withColumnRenamed('base_sponsor_key', 'CDE_BASE_SPONSOR_KEY') \
    .withColumnRenamed('base_bu_key', 'CDE_BASE_BU_KEY') \
    .withColumnRenamed('base_opco_key', 'CDE_BASE_OPCO_KEY') \
    .withColumnRenamed('layered_sponsor_key', 'CDE_LAYERED_SPONSOR_KEY') \
    .withColumnRenamed('layered_bu_key', 'CDE_LAYERED_BU_KEY') \
    .withColumnRenamed('layered_opco_key', 'CDE_LAYERED_OPCO_KEY') \
    .withColumnRenamed('trx_type', 'DES_TRANSACTION_TYPE') \
    .withColumnRenamed('trx_source', 'DES_TRANSACTION_SOURCE') \
    .withColumnRenamed('original_trx_key', 'CDE_ORIGINAL_TXN_KEY') \
    .withColumnRenamed('pay_in_currency', 'CUA_PAY_IN_CURRENCY') \
    .withColumnRenamed('pay_in_points_value', 'CUA_PAY_IN_POINTS_VALUE') \
    .withColumnRenamed('tax_amount', 'CUA_TAX_AMOUNT') \
    .withColumnRenamed('payment_method_key', 'CDE_PAYMENT_METHOD_KEY') \
    .withColumnRenamed('points_earned_basic', 'QTY_POINTS_EARNED_BASIC') \
    .withColumnRenamed('points_earned_status', 'QTY_POINTS_EARNED_STATUS') \
    .withColumnRenamed('points_redeemed_basic', 'QTY_REDEEMED_BASIC') \
    .withColumnRenamed('points_redeemed_status', 'QTY_REDEEMED_STATUS') \
    .withColumnRenamed('base_amount_points', 'CUA_BASE_AMOUNT_POINTS') \
    .withColumnRenamed('layered_amount_points', 'CUA_LAYERED_AMOUNT_POINTS') \
    .withColumnRenamed('bonus_amount_points', 'CUA_BONUS_AMOUNT_POINTS') \
    .withColumnRenamed('count_bonus_offers', 'QTY_BONUS_OFFERS') \
    .withColumnRenamed('line_position,', 'CDE_LINE_POSITION') \
    .withColumnRenamed('error_code', 'CDE_ERROR') \
    .withColumnRenamed('error_message', 'DES_ERROR') \
    .withColumnRenamed('is_test', 'IND_IS_TEST') \
    .withColumnRenamed('is_error', 'IND_IS_ERROR') \
    .withColumnRenamed('is_manual', 'IND_IS_MANUAL') \
    .withColumnRenamed('is_cancellation', 'IND_IS_CANCELLATION') \
    .withColumnRenamed('is_reversed', 'IND_IS_REVERSED') \
    .withColumnRenamed('is_purchase', 'IND_IS_PURCHASE') \
    .withColumnRenamed('is_third_party', 'IND_IS_THIRD_PARTY') \
    .withColumnRenamed('load_id', 'IDI_LOAD') \
    .withColumnRenamed('ods_id', 'IDI_ODS')

# rename all columns to lower case
for col_name in trx_full.columns:
    trx_full = trx_full.withColumnRenamed(col_name, col_name.lower())

missing_columns = [
    "dat_batch",
    "dat_period_start",
    "dat_period_end",
    "cod_sor_turnover",
    "cdi_period_date_type",
    "ind_counterparty_type",
    "cod_sor_counterparty",
    "idi_owner",
    "cod_sor_owner",
    "idi_owner_2",
    "cod_sor_owner_2",
    "cod_sor_proposition",
    "cdi_turnover_status",
    "des_turnover_status",
    "cdi_date_type_1",
    "cdi_date_type_2",
    "dat_date_type_2",
    "cdi_date_type_3",
    "dat_date_type_3",
    "qty_number_of_items",
    "cdi_item_unit",
    "cdi_amount_type_1",
    "cdi_amount_type_2",
    "cua_amount_type_3",
    "cdi_amount_channel",
    "des_amount_channel",
    "att_turnover_classification_1",
    "cdi_turnover_classification_1",
    "des_turnover_classification_1",
    "att_turnover_classification_2",
    "cdi_turnover_classification_2",
    "des_turnover_classification_2",
    "att_turnover_classification_3",
    "cdi_turnover_classification_3",
    "des_turnover_classification_3",
    "idi_promotion",
    "cod_sor_promotion",
]

# add missing columns with empty values
for c in missing_columns:
    trx_full = trx_full.withColumn(c, lit(None).cast(StringType()))

target_columns = {'dat_batch', 'dat_period_start', 'dat_period_end', 'idi_turnover', 'cod_sor_turnover',
                  'cdi_period_date_type', 'ind_counterparty_type', 'idi_counterparty', 'cod_sor_counterparty',
                  'idi_owner', 'cod_sor_owner', 'idi_owner_2', 'cod_sor_owner_2', 'idi_proposition',
                  'cod_sor_proposition', 'cdi_turnover_status', 'des_turnover_status', 'cdi_date_type_1',
                  'dat_date_type_1', 'cdi_date_type_2', 'dat_date_type_2', 'cdi_date_type_3', 'dat_date_type_3',
                  'qty_number_of_items', 'cdi_item_unit', 'cde_currency', 'cdi_amount_type_1', 'cua_amount_type_1',
                  'cdi_amount_type_2', 'cua_amount_type_2', 'cua_amount_type_3', 'cdi_amount_channel',
                  'des_amount_channel', 'att_turnover_classification_1', 'cdi_turnover_classification_1',
                  'des_turnover_classification_1', 'att_turnover_classification_2', 'cdi_turnover_classification_2',
                  'des_turnover_classification_2', 'att_turnover_classification_3', 'cdi_turnover_classification_3',
                  'des_turnover_classification_3', 'idi_promotion', 'cod_sor_promotion'}

# we drop any additional (potentially valuable intput) to match the landing schema
columns_to_drop = [c for c in trx_full.columns if c not in target_columns]
for c in columns_to_drop:
    trx_full = trx_full.drop(c)

trx_full = trx_full.fillna('')

# cast everything to string
for col_name in trx_full.columns:
    trx_full = trx_full.withColumn(col_name, col(col_name).cast(StringType()))

trx_full.write \
    .mode('overwrite') \
    .parquet('s3://cvm-landing-a6623c3/transactions/share/2022-12-18/')

job.commit()
# spark-submit transform_transactions.py --JOB_NAME=transform_transactions
