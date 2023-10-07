from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *

from typing import Iterable


def melt_df(
        df: DataFrame, id_vars: Iterable[str], value_vars: Iterable[str],
        var_name: str = "variable", value_name: str = "value"
) -> DataFrame:
    """Convert :class:`DataFrame` from wide to long format."""

    # Create array<struct<variable: str, value: ...>>
    _vars_and_vals = array(*(
        struct(lit(c).alias(var_name), col(c).alias(value_name))
        for c in value_vars))

    # Add to the DataFrame and explode
    _tmp = df.withColumn("_vars_and_vals", explode(_vars_and_vals))

    cols = id_vars + [col("_vars_and_vals")[x].alias(x) for x in [var_name, value_name]]
    return _tmp.select(*cols)


args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

spark = glueContext.spark_session

ids = spark.read.csv(
    path="s3://cvm-develop/data_extracts/id_mapping/20220721/ods_gcr_id_mapping_d33f830b.csv",
    sep='|',
    schema=StructType([
        StructField("gcr_id", StringType(), True),
        StructField("mafid", StringType(), True),
        StructField("share_id", StringType(), True),
        StructField("crf_id", StringType(), True),
        StructField("lne_id", StringType(), True),
        StructField("vox_id", StringType(), True),
        StructField("njm_id", StringType(), True),
        StructField("ecp_id", StringType(), True),
        StructField("fsn_id", StringType(), True),
        StructField("wfi_id", StringType(), True),
        StructField("ni_card_index", StringType(), True)
    ])
)

columns = ['gcr_id', 'share_id', 'crf_id', 'vox_id']

for c in columns:
    ids = ids.withColumn(c, trim(col(c)))

nonempty_ids = ids.select(columns).filter(
    col('gcr_id').isNotNull() & (
            col('share_id').isNotNull() |
            col('crf_id').isNotNull() |
            col('vox_id').isNotNull()
    )
)

nonempty_ids = nonempty_ids \
    .withColumnRenamed(existing="gcr_id", new='idi_gcr') \
    .withColumnRenamed(existing="share_id", new='share') \
    .withColumnRenamed(existing="crf_id", new='crf') \
    .withColumnRenamed(existing="vox_id", new='vox')

melted_ids = melt_df(nonempty_ids, ['idi_gcr'], ['share', 'crf', 'vox'], 'cod_sor_idi_src', 'idi_src')
melted_ids = melted_ids.filter(col('idi_src').isNotNull()).drop_duplicates()

melted_ids = melted_ids \
    .withColumn('dat_created', current_date()) \
    .withColumn('dat_updated', current_date()) \
    .withColumn('cod_sor_idi_gcr', lit('ods_gcr_id_mapping_d33f830b')) \
    .withColumn('bu', col('cod_sor_idi_src')) \
    .withColumn('ingestion_date', current_date())

melted_ids.write \
    .mode('overwrite') \
    .partitionBy('bu', 'ingestion_date') \
    .parquet('s3://cvm-landing-a6623c3/id_mapping/')
