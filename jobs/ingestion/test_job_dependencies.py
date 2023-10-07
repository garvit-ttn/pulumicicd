import sys

from awsglue import DynamicFrame
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions

from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# from delta import DeltaTable

from feature_store_pyspark.FeatureStoreManager import FeatureStoreManager
import feature_store_pyspark

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

spark = glueContext.spark_session

print('running')
extra_jars = ",".join(feature_store_pyspark.classpath_jars())
print(extra_jars)

# Construct test DataFrame
columns = ["id", "timestamp", "col1", "col2", "col3"]
data = [
    ("1", "2021-03-02T12:20:12Z", "xyz01", "abc01", "poi01"),
    ("2", "2021-03-02T12:20:13Z", "xyz02", "abc02", "poi02"),
    ("3", "2021-03-02T12:20:14Z", "xyz03", "abc03", "poi03")
]
df = spark.createDataFrame(data).toDF(*columns)

# Initialize FeatureStoreManager with a role arn if your feature group is created by another account
feature_store_manager = FeatureStoreManager(
    assume_role_arn='arn:aws:iam::724711057192:role/cvm-sagemaker-execution-role-12a1a45'
)
#
# If only OfflineStore is selected, the connector will batch write the data to offline store directly
feature_group_arn = "arn:aws:sagemaker:eu-west-1:724711057192:feature-group/cvm-features-core"
feature_store_manager.ingest_data(
    input_data_frame=df,
    feature_group_arn=feature_group_arn,
    target_stores=["OfflineStore"]
)

# To retrieve the records failed to be ingested by spark connector
# failed_records_df = feature_store_manager.get_failed_stream_ingestion_data_frame()
# print(failed_records_df.show(5))

job.commit()
