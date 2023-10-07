from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import *

from cvmdatalake import conformed
from cvmdatalake.synthetic import fake_df

args = getResolvedOptions(
    sys.argv,
    ['JOB_NAME', 'lake_descriptor', 'cvm_environment', 'sample_entries', 'sample_columns']
)

sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
spark = glueContext.spark_session

lake_descriptor = args['lake_descriptor']
environment = args['cvm_environment']

sample_entries = int(args['sample_entries'])
sample_columns = int(args['sample_columns'])

sample_entries_name = ""
if 0 <= sample_entries < 1000:
    sample_entries_name = f"{sample_entries}"
elif 1000 <= sample_entries < 1000000:
    sample_entries_name = f"{int(sample_entries / 1000)}k"
elif sample_entries >= 1000000:
    sample_entries_name = f"{int(sample_entries / 1000000)}M"

profile_columns = len(conformed.CustomerProfile.fields())
print(f"Core faked profile columns: {profile_columns}")

missing_columns = sample_columns - profile_columns
missing_string_columns = int(missing_columns / 2)
missing_float_columns = missing_columns - missing_string_columns

print(f"Missing additional columns to generate: {missing_columns}")
print(f"Missing string columns to generate: {missing_string_columns}")
print(f"Missing float columns to generate: {missing_float_columns}")

sample_profiles_df = fake_df(
    spark, conformed.CustomerProfile, n=sample_entries,
    n_fake_string_columns=missing_string_columns, n_fake_float_columns=missing_float_columns
)

(
    sample_profiles_df.write.format('delta').mode('overwrite')
    .partitionBy('bu')
    .save(f"s3://cvm-krz-conformed-7a4f071/sample_profiles/{sample_entries_name}_{sample_columns}")
)
