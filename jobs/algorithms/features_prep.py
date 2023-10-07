from itertools import chain

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from delta.tables import *
from pyspark.sql.functions import *

sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
job.init(args['JOB_NAME'], args)
spark = glueContext.spark_session


def columns_to_upper(df: DataFrame) -> DataFrame:
    for c in df.columns:
        df = df.withColumnRenamed(c, c.upper())
    return df


##########################################
# Columns definitions
##########################################
nbo_training_columns = {'idi_counterparty', 'idi_proposition'}
nbo_inference_columns = {'idi_counterparty'}

# missing 'category' - where does it come from?
# using trx_date instead of dat_order_date
# using cua_amount_type_2 instead of cua_net_amount
rfm_training_columns = {'idi_counterparty', 'trx_date', 'cua_amount_type_2'}
rfm_inference_columns = {'idi_counterparty'}

# using trx_date instead of dat_order_date
# using cua_amount_type_2 instead of cua_net_amount
clv_training_columns = {'idi_counterparty', 'trx_date', 'cua_amount_type_2'}
clv_inference_columns = {'idi_counterparty', 'trx_date', 'cua_amount_type_2'}

# we don't have a valid transaction id at the item level
# idi_turnover is ID at the header level - it's not unique across all transactions
mba_training_columns = {'idi_turnover', 'idi_proposition'}
mba_inference_columns = {'idi_turnover', 'idi_proposition'}

id_columns = {'idi_src', 'idi_gcr'}

all_trx_columns = set(chain(
    nbo_training_columns, rfm_training_columns, clv_training_columns, mba_training_columns,
    nbo_inference_columns, rfm_inference_columns, clv_inference_columns, mba_inference_columns
))

all_columns = set(chain(
    id_columns, all_trx_columns
))

##########################################
# GCR mapping
##########################################
id_mapping_df = DeltaTable.forPath(spark, "s3://cvm-conformed-02d3063/core_id_mapping").toDF()
transactions_df = DeltaTable.forPath(spark, "s3://cvm-conformed-02d3063/core_transactions").toDF()

gcr_transactions_df = transactions_df \
    .select(*all_trx_columns) \
    .join(id_mapping_df.select(*id_columns), (transactions_df.idi_counterparty == id_mapping_df.idi_src), 'left') \
    .select(*all_columns) \
    .drop('idi_counterparty') \
    .withColumnRenamed(existing='idi_gcr', new='idi_counterparty')

##########################################
# write training features
##########################################
nbo_training_features_df = gcr_transactions_df.select(*nbo_training_columns)
nbo_training_features_df = columns_to_upper(nbo_training_features_df)
nbo_training_features_df.write.mode('overwrite').csv('s3://cvm-conformed-02d3063/features/training/nbo', header=True)

rfm_training_features_df = gcr_transactions_df.select(*rfm_training_columns) \
    .withColumnRenamed(existing='trx_date', new='dat_order_date') \
    .withColumnRenamed(existing='cua_amount_type_2', new='cua_net_amount') \
    .withColumn('category', lit(''))
rfm_training_features_df = columns_to_upper(rfm_training_features_df)
rfm_training_features_df.write.mode('overwrite').csv('s3://cvm-conformed-02d3063/features/training/rfm', header=True)

clv_training_features_df = gcr_transactions_df.select(*clv_training_columns) \
    .withColumnRenamed(existing='trx_date', new='dat_order_date') \
    .withColumnRenamed(existing='cua_amount_type_2', new='cua_net_amount')
clv_training_features_df = columns_to_upper(clv_training_features_df)
clv_training_features_df.write.mode('overwrite').csv('s3://cvm-conformed-02d3063/features/training/clv', header=True)

mba_training_features_df = gcr_transactions_df.select(*mba_training_columns)
mba_training_features_df = columns_to_upper(mba_training_features_df)
mba_training_features_df.write.mode('overwrite').csv('s3://cvm-conformed-02d3063/features/training/mba/', header=True)

# sat_training_features_df = todo
# nbp_training_features_df = todo


##########################################
# write inference features
##########################################
nbo_inference_features_df = gcr_transactions_df.select(*nbo_inference_columns)
nbo_inference_features_df = columns_to_upper(nbo_inference_features_df)
nbo_inference_features_df.write.mode('overwrite').csv('s3://cvm-conformed-02d3063/features/inference/nbo/', header=True)

rfm_inference_features_df = gcr_transactions_df.select(*rfm_inference_columns)
rfm_inference_features_df = columns_to_upper(rfm_inference_features_df)
rfm_inference_features_df.write.mode('overwrite').csv('s3://cvm-conformed-02d3063/features/inference/rfm/', header=True)

clv_inference_features_df = gcr_transactions_df.select(*clv_inference_columns) \
    .withColumnRenamed(existing='trx_date', new='dat_order_date') \
    .withColumnRenamed(existing='cua_amount_type_2', new='cua_net_amount')
clv_inference_features_df = columns_to_upper(clv_inference_features_df)
clv_inference_features_df.write.mode('overwrite').csv('s3://cvm-conformed-02d3063/features/inference/clv/', header=True)

mba_inference_features_df = gcr_transactions_df.select(*mba_inference_columns)
mba_inference_features_df = columns_to_upper(mba_inference_features_df)
mba_inference_features_df.write.mode('overwrite').csv('s3://cvm-conformed-02d3063/features/inference/mba/', header=True)

# sat_inference_features_df = todo
# nbp_inference_features_df = todo
