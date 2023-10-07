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

path_to_files = get_s3_path_for_feature(features.ClvTrainingInput, lake_descriptor, data_filter_context)

#  CLV
clv = spark.read.option("header", True).csv(path_to_files)
clv = clv.withColumn("data_filter_context", concat(lit(data_filter_context), lit('_'), lit(current_date().cast(StringType()))))
clv = clv.withColumn("date_of_prepare", lit(current_date()))
clv.show()

update_map = {
    conformed.ProfileAlgorithmsOutput.date_of_prepare.column_alias(): "clv.date_of_prepare",
    conformed.ProfileAlgorithmsOutput.clv_frequency.column_alias(): "clv.FREQUENCY",
    conformed.ProfileAlgorithmsOutput.clv_recency.column_alias(): "clv.RECENCY",
    conformed.ProfileAlgorithmsOutput.clv_t.column_alias(): "clv.T",
    conformed.ProfileAlgorithmsOutput.clv_6_months_expected_purchases.column_alias(): "clv.1_MONTHS_EXPECTED_PURCHASES",
    conformed.ProfileAlgorithmsOutput.clv_6_months_clv.column_alias(): "clv.1_MONTHS_CLV",
    conformed.ProfileAlgorithmsOutput.clv_6_prob_alive.column_alias(): "clv.PROB_ALIVE"
}
insert_map = {
    conformed.ProfileAlgorithmsOutput.data_filter_context.column_alias(): "clv.data_filter_context",
    conformed.ProfileAlgorithmsOutput.idi_counterparty.column_alias(): "clv.IDI_COUNTERPARTY",
    conformed.ProfileAlgorithmsOutput.date_of_prepare.column_alias(): "clv.date_of_prepare",
    conformed.ProfileAlgorithmsOutput.clv_frequency.column_alias(): "clv.FREQUENCY",
    conformed.ProfileAlgorithmsOutput.clv_recency.column_alias(): "clv.RECENCY",
    conformed.ProfileAlgorithmsOutput.clv_t.column_alias(): "clv.T",
    conformed.ProfileAlgorithmsOutput.clv_6_months_expected_purchases.column_alias(): "clv.1_MONTHS_EXPECTED_PURCHASES",
    conformed.ProfileAlgorithmsOutput.clv_6_months_clv.column_alias(): "clv.1_MONTHS_CLV",
    conformed.ProfileAlgorithmsOutput.clv_6_prob_alive.column_alias(): "clv.PROB_ALIVE"
}

profileAlgorithmsOutput.alias(conformed.ProfileAlgorithmsOutput.table_alias()).merge(
    clv.alias("clv"),
    f"""
    {conformed.ProfileAlgorithmsOutput.data_filter_context.column_alias()} = {"clv.data_filter_context"} AND
    {conformed.ProfileAlgorithmsOutput.idi_counterparty.column_alias()} = {"clv.IDI_COUNTERPARTY"}
    """
).whenMatchedUpdate(set=update_map).whenNotMatchedInsert(values=insert_map).execute()

profileAlgorithmsOutput.toDF().show()
