import datetime

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import *
from pyspark.sql.functions import substring, length, col, expr
from functools import reduce
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

path_to_files = get_s3_path_for_feature(features.MbaOutput, lake_descriptor, data_filter_context)



#  MBA
mba = spark.read.option("header", True).csv(path_to_files)
mba = mba.withColumn("data_filter_context", concat(lit(data_filter_context), lit('_'), lit(current_date().cast(StringType()))))
mba = mba.withColumn("date_of_prepare", lit(current_date()))

mba.show()


current_columns = mba.columns # Returns list of columns as python list
new_columns = list(map(lambda item : item.replace(" ","_").upper(),current_columns))

mba = reduce(lambda data, idx: data.withColumnRenamed(current_columns[idx], new_columns[idx]), range(len(current_columns)), mba)

mba = mba.withColumn("IDI_COUNTERPARTY",expr("substring(IDI_TRANSACTION, 1, length(IDI_TRANSACTION)-8)"))

mba.show()
mba.printSchema()


update_map = {
    conformed.ProfileAlgorithmsOutput.mba_idi_transaction.column_alias(): "mba.IDI_TRANSACTION",
    conformed.ProfileAlgorithmsOutput.mba_items.column_alias(): "mba.ITEMS",
    conformed.ProfileAlgorithmsOutput.mba_antecedents.column_alias(): "mba.ANTECEDENTS",
    conformed.ProfileAlgorithmsOutput.mba_consequents.column_alias(): "mba.CONSEQUENTS",
    conformed.ProfileAlgorithmsOutput.mba_antecedent_support.column_alias(): "mba.ANTECEDENT_SUPPORT",
    conformed.ProfileAlgorithmsOutput.mba_consequent_support.column_alias(): "mba.CONSEQUENT_SUPPORT",
    conformed.ProfileAlgorithmsOutput.mba_support.column_alias(): "mba.SUPPORT",
    conformed.ProfileAlgorithmsOutput.mba_confidence.column_alias(): "mba.CONFIDENCE",
    conformed.ProfileAlgorithmsOutput.mba_lift.column_alias(): "mba.LIFT",
    conformed.ProfileAlgorithmsOutput.mba_leverage.column_alias(): "mba.LEVERAGE",
    conformed.ProfileAlgorithmsOutput.mba_conviction.column_alias(): "mba.CONVICTION"
}
insert_map = {
    conformed.ProfileAlgorithmsOutput.data_filter_context.column_alias(): "mba.data_filter_context",
    conformed.ProfileAlgorithmsOutput.idi_counterparty.column_alias(): "mba.IDI_COUNTERPARTY",
    conformed.ProfileAlgorithmsOutput.date_of_prepare.column_alias(): "mba.date_of_prepare",
    conformed.ProfileAlgorithmsOutput.mba_idi_transaction.column_alias(): "mba.IDI_TRANSACTION",
    conformed.ProfileAlgorithmsOutput.mba_items.column_alias(): "mba.ITEMS",
    conformed.ProfileAlgorithmsOutput.mba_antecedents.column_alias(): "mba.ANTECEDENTS",
    conformed.ProfileAlgorithmsOutput.mba_consequents.column_alias(): "mba.CONSEQUENTS",
    conformed.ProfileAlgorithmsOutput.mba_antecedent_support.column_alias(): "mba.ANTECEDENT_SUPPORT",
    conformed.ProfileAlgorithmsOutput.mba_consequent_support.column_alias(): "mba.CONSEQUENT_SUPPORT",
    conformed.ProfileAlgorithmsOutput.mba_support.column_alias(): "mba.SUPPORT",
    conformed.ProfileAlgorithmsOutput.mba_confidence.column_alias(): "mba.CONFIDENCE",
    conformed.ProfileAlgorithmsOutput.mba_lift.column_alias(): "mba.LIFT",
    conformed.ProfileAlgorithmsOutput.mba_leverage.column_alias(): "mba.LEVERAGE",
    conformed.ProfileAlgorithmsOutput.mba_conviction.column_alias(): "mba.CONVICTION"
}

profileAlgorithmsOutput.alias(conformed.ProfileAlgorithmsOutput.table_alias()).merge(
    mba.alias("mba"),
    f"""
    {conformed.ProfileAlgorithmsOutput.data_filter_context.column_alias()} = {"mba.data_filter_context"} AND
    {conformed.ProfileAlgorithmsOutput.idi_counterparty.column_alias()} = {"mba.IDI_COUNTERPARTY"}
    """
).whenMatchedUpdate(set=update_map).whenNotMatchedInsert(values=insert_map).execute()

profileAlgorithmsOutput.toDF().show()
