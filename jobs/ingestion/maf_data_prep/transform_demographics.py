from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *

from typing import Iterable

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

spark = glueContext.spark_session

crf_demographics_df = spark.read.option("recursiveFileLookup", "true").load(
    path='s3://cvm-develop/vertica_to_cvm/demographics/crf/',
    format='parquet'
)

share_demographics_df = spark.read.option("recursiveFileLookup", "true").load(
    path='s3://cvm-develop/vertica_to_cvm/demographics/share/',
    format='parquet'
)

gcr_demographics_df = spark.read.option("recursiveFileLookup", "true").load(
    path='s3://cvm-develop/vertica_to_cvm/demographics/gcr/',
    format='parquet'
)

crf_demographics_df = crf_demographics_df.withColumn('bu', lit('crf'))
share_demographics_df = share_demographics_df.withColumn('bu', lit('share'))
gcr_demographics_df = gcr_demographics_df.withColumn('bu', lit('gcr'))

demographics_df = crf_demographics_df.union(share_demographics_df).union(gcr_demographics_df) \
    .withColumn('ingestion_date', current_date().cast(StringType()))

for c in demographics_df.columns:
    demographics_df = demographics_df.withColumnRenamed(c, c.lower())

#  covert identifier to string
for c in ['cod_sor_counterparty', 'cod_sor_owner', 'cod_sor_owner_2']:
    demographics_df = demographics_df.withColumn(c, col(c).cast(StringType()))

# covert string to days
for c in ['dat_snapshot', 'dat_person_optin', 'dat_person_telephonenumber_optin', 'dat_person_telephonenumber_optin_2',
          'dat_person_email_optin', 'dat_person_sms_optin', 'dat_of_birth', ]:
    demographics_df = demographics_df.withColumn(c, to_date(c))

demographics_df.write \
    .mode('overwrite') \
    .partitionBy('bu', 'ingestion_date') \
    .parquet('s3://cvm-landing-a6623c3/demographics')
