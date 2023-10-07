from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from delta import DeltaTable
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType

from cvmdatalake import conformed, creat_delta_table_if_not_exists, get_s3_path, create_dynamic_frame_from_catalog

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'lake_descriptor', 'cvm_environment'])

sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

spark = glueContext.spark_session
lake_descriptor = args['lake_descriptor']
cvm_environment = args['cvm_environment']

creat_delta_table_if_not_exists(spark, conformed.Predictions, lake_descriptor)

nbo_output: DataFrame = spark.read.options(header=True, sep=',').csv(
    path="s3://cvm-dev-conformed-cc546fe/features/nbo/output/test_nbo_output.csv",
    multiLine=True
)

# nbo_output: DataFrame = create_dynamic_frame_from_catalog(
#     glue_context=glueContext,
#     table=conformed.NboOutput,
#     prefix=cvm_environment,
# ).toDF()

# rfm_output: DataFrame = spark.read.options(header=True, sep=',').csv(
#     path="s3://cvm-dev-conformed-cc546fe/features/rfm/output/test_rfm_output.csv",
#     multiLine=True
# ).toDF()
#
# rfm_output = rfm_output.withColumnRenamed(
#     existing=conformed.RfmOutput.r_rank_norm.name,
#     new=conformed.Predictions.recency_norm.name
# ).withColumnRenamed(
#     existing=conformed.RfmOutput.f_rank_norm.name,
#     new=conformed.Predictions.frequency_norm.name
# ).withColumnRenamed(
#     existing=conformed.RfmOutput.m_rank_norm.name,
#     new=conformed.Predictions.monetary_norm.name
# ).drop(conformed.RfmOutput.idi_proposition_level03.name)

# rfm_output: DataFrame = create_dynamic_frame_from_catalog(
#     glue_context=glueContext,
#     table=conformed.RfmOutput,
#     prefix=cvm_environment,
# ).toDF()

nbo_output = nbo_output.withColumnRenamed(
    existing=conformed.NboOutput.idi_proposition.name,
    new=conformed.Predictions.next_best_offer.name
)

# nbo_output = nbo_output.alias(conformed.NboOutput.table_name()).join(
#     rfm_output.alias(conformed.RfmOutput.table_name()),
#     col(
#         conformed.NboOutput.idi_counterparty.column_id_for_join()
#     ) == col(
#         conformed.RfmOutput.idi_counterparty.column_id_for_join()),
#     'left'
# )

for col in conformed.Predictions:
    if col not in [
        conformed.Predictions.next_best_offer,
        conformed.Predictions.idi_counterparty,
        conformed.Predictions.category.name
    ]:
        spark_type = IntegerType() if col.value.data_type == 'int' else StringType()
        nbo_output = nbo_output.withColumn(col.name, lit(None).cast(spark_type))

nbo_output = nbo_output.withColumn(conformed.Predictions.category.name, lit('all'))

conformed_predictions_path = get_s3_path(conformed.Predictions, lake_descriptor)
conformed_predictions = DeltaTable.forPath(spark, conformed_predictions_path)
conformed_predictions.alias('pred').merge(
    nbo_output.alias('nbo_out'),
    f"""
    nbo_out.{conformed.NboOutput.idi_counterparty.name} = pred.{conformed.Predictions.idi_counterparty.name}
    """
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

job.commit()
