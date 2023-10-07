import datetime

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import *
from pyspark.sql.types import DateType, StringType

from cvmdatalake.data_filters import register_data_filter, cvm_data_filter_registry, DataFilterContext
from cvmdatalake import create_delta_table_from_catalog, conformed, get_s3_path, features, upsert_to_deltatable, \
    get_s3_path_for_feature

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'lake_descriptor', 'cvm_environment', 'data_filter_context'])

sc = SparkContext()
glue_context = GlueContext(sc)
job = Job(glue_context)
job.init(args['JOB_NAME'], args)

spark = glue_context.spark_session
lake_descriptor = args['lake_descriptor']
cvm_environment = args['cvm_environment']
data_filter_context = args['data_filter_context']

profileAlgorithmsOutput = create_delta_table_from_catalog(spark, conformed.ProfileAlgorithmsOutput, lake_descriptor)

path_to_files = get_s3_path_for_feature(features.RfmOutput, lake_descriptor, data_filter_context)

#  RFM
rfm = spark.read.option("header", True).csv(path_to_files)
rfm = rfm.withColumn("data_filter_context", concat(lit(data_filter_context), lit('_'), lit(current_date().cast(StringType()))))
rfm = rfm.withColumn("date_of_prepare", lit(current_date()))
rfm.show()


rfm = rfm.dropDuplicates([conformed.ProfileAlgorithmsOutput.idi_counterparty.name])

update_map = {
    conformed.ProfileAlgorithmsOutput.date_of_prepare.column_alias(): "rfm.date_of_prepare",
    conformed.ProfileAlgorithmsOutput.rfm_recency.column_alias(): "rfm.RECENCY",
    conformed.ProfileAlgorithmsOutput.rfm_frequency.column_alias(): "rfm.FREQUENCY",
    conformed.ProfileAlgorithmsOutput.rfm_r_rank_norm.column_alias(): "rfm.R_RANK_NORM",
    conformed.ProfileAlgorithmsOutput.rfm_f_rank_norm.column_alias(): "rfm.F_RANK_NORM",
    conformed.ProfileAlgorithmsOutput.rfm_m_rank_norm.column_alias(): "rfm.M_RANK_NORM",
    conformed.ProfileAlgorithmsOutput.rfm_customer_segment.column_alias(): "rfm.CUSTOMER_SEGMENT"
}
insert_map = {
    conformed.ProfileAlgorithmsOutput.data_filter_context.column_alias(): "rfm.data_filter_context",
    conformed.ProfileAlgorithmsOutput.idi_counterparty.column_alias(): "rfm.IDI_COUNTERPARTY",
    conformed.ProfileAlgorithmsOutput.date_of_prepare.column_alias(): "rfm.date_of_prepare",
    conformed.ProfileAlgorithmsOutput.rfm_recency.column_alias(): "rfm.RECENCY",
    conformed.ProfileAlgorithmsOutput.rfm_frequency.column_alias(): "rfm.FREQUENCY",
    conformed.ProfileAlgorithmsOutput.rfm_r_rank_norm.column_alias(): "rfm.R_RANK_NORM",
    conformed.ProfileAlgorithmsOutput.rfm_f_rank_norm.column_alias(): "rfm.F_RANK_NORM",
    conformed.ProfileAlgorithmsOutput.rfm_m_rank_norm.column_alias(): "rfm.M_RANK_NORM",
    conformed.ProfileAlgorithmsOutput.rfm_customer_segment.column_alias(): "rfm.CUSTOMER_SEGMENT"
}

profileAlgorithmsOutput.alias(conformed.ProfileAlgorithmsOutput.table_alias()).merge(
    rfm.alias("rfm"),
    f"""
    {conformed.ProfileAlgorithmsOutput.data_filter_context.column_alias()} = {"rfm.data_filter_context"} AND
    {conformed.ProfileAlgorithmsOutput.idi_counterparty.column_alias()} = {"rfm.IDI_COUNTERPARTY"}
    """
).whenMatchedUpdate(set=update_map).whenNotMatchedInsert(values=insert_map).execute()

profileAlgorithmsOutput.toDF().show()
