import datetime

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import *
from pyspark.sql.types import DateType, StringType
from pyspark.sql.window import Window as W
from pyspark.sql import functions as F

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

path_to_files = get_s3_path_for_feature(features.NboOutput, lake_descriptor, data_filter_context)

#  NBO
nbo = spark.read.option("header", True).csv(path_to_files)
nbo = nbo.withColumn("data_filter_context", concat(lit(data_filter_context), lit('_'), lit(current_date().cast(StringType()))))
nbo = nbo.withColumn("date_of_prepare", lit(current_date()))

windowSpec = W.partitionBy("IDI_COUNTERPARTY").orderBy("IDI_COUNTERPARTY")
nbo = nbo.withColumn("RANK", F.row_number().over(windowSpec))


nbo.show()

update_map = {
    conformed.ProfileAlgorithmsOutput.date_of_prepare.column_alias(): "nbo.date_of_prepare",
    conformed.ProfileAlgorithmsOutput.nbo_json.column_alias(): "nbo.NBO",
    conformed.ProfileAlgorithmsOutput.nbo_rank.column_alias(): "nbo.RANK"
}
insert_map = {
    conformed.ProfileAlgorithmsOutput.data_filter_context.column_alias(): "nbo.data_filter_context",
    conformed.ProfileAlgorithmsOutput.idi_counterparty.column_alias(): "nbo.IDI_COUNTERPARTY",
    conformed.ProfileAlgorithmsOutput.date_of_prepare.column_alias(): "nbo.date_of_prepare",
    conformed.ProfileAlgorithmsOutput.nbo_json.column_alias(): "nbo.NBO",
    conformed.ProfileAlgorithmsOutput.nbo_rank.column_alias(): "nbo.RANK"
}

profileAlgorithmsOutput.alias(conformed.ProfileAlgorithmsOutput.table_alias()).merge(
    nbo.alias("nbo"),
    f"""
    {conformed.ProfileAlgorithmsOutput.data_filter_context.column_alias()} = {"nbo.data_filter_context"} AND
    {conformed.ProfileAlgorithmsOutput.idi_counterparty.column_alias()} = {"nbo.IDI_COUNTERPARTY"}
    """
).whenMatchedUpdate(set=update_map).whenNotMatchedInsert(values=insert_map).execute()

profileAlgorithmsOutput.toDF().show()
