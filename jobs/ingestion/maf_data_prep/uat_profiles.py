from awsglue import DynamicFrame
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from delta.tables import *
from pyspark.sql.functions import *
from pyspark.sql.types import LongType

from cvmdatalake import creat_delta_table_if_not_exists, landing, get_s3_path, conformed
from cvmdatalake.conformed import CustomerProfile, CustomAttributes

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'lake_descriptor', 'cvm_environment'])

sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
spark = glueContext.spark_session

lake_descriptor = args['lake_descriptor']
environment = args['cvm_environment']

# ids = ['EXSTESTTB', 'EXSTESTMA', 'EXSTESTNR', 'EXSTESTSA', 'EXSTESTSA', 'EXSTESTSA', 'EXSTESTMG']
#
# profiles_path = get_s3_path(CustomerProfile, lake_descriptor)
#
# profiles = spark.read.format('delta').load(profiles_path).filter(
#     col(CustomerProfile.idi_counterparty.name) == '100412')
#
# all_to_insert = None
# for idi in ids:
#     to_insert = (
#         profiles.withColumn(CustomerProfile.des_age.name, lit(500).cast(LongType()))
#         .withColumn(CustomerProfile.idi_counterparty.name, lit(idi))
#     )
#
#     to_insert.show(5)
#     all_to_insert = to_insert if all_to_insert is None else all_to_insert.union(to_insert)
#
# all_to_insert.show(10)
# all_to_insert.write.format('delta').mode('append').save(profiles_path)

braze_path = get_s3_path(CustomAttributes, lake_descriptor)

idis = {
    'EXSTESTTB': 'SVVuQdMLFALMjI8tg1dHnA==',
    'EXSTESTMA': 'Vj4aB62NVuABazrjrBjT0Q==',
    'EXSTESTNR': 'awBKSRfOuO5jCpnWKTrKAA==',
    'EXSTESTSA': 'VfY8/t2/n7ygiJgViefIjw==',
    'EXSTESTAS': '9Uz+BaIawK69e81GF+qGDA==',
    'EXSTESTFE': 'EfNGRx/G5R9ex4aQpuE36A==',
    'EXSTESTMG': 'Q/OzYrClWtgnYZUQskS/aQ==',
}

braze_entries = spark.read.format('delta').load(braze_path).filter(
    col(CustomAttributes.idi_counterparty.name) == '7121859')

all_to_insert = None
for k, v in idis.items():
    to_insert = (
        braze_entries
        .withColumn(CustomAttributes.idi_counterparty.name, lit(k))
        .withColumn(CustomAttributes.external_id.name, lit(v))
    )

    to_insert.show(5)
    all_to_insert = to_insert if all_to_insert is None else all_to_insert.union(to_insert)

all_to_insert.show(10)
all_to_insert.write.format('delta').mode('append').save(braze_path)
