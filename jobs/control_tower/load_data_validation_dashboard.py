from awsglue import DynamicFrame
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from delta.tables import DeltaTable
from pyspark.sql.functions import *
from pyspark.sql.types import StringType

from cvmdatalake import landing, conformed, create_delta_table_from_catalog, create_dynamic_frame_from_catalog,get_s3_path

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'lake_descriptor', 'cvm_environment', 'rds_cvm_connection_name', 'cvm_rds_db_name'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

rds_connection = args['rds_cvm_connection_name']
lake_descriptor = args['lake_descriptor']
environment = args['cvm_environment']
rds_db_name = args['cvm_rds_db_name']

rds_db_name = 'xxxxxx'
rds_db_table = 'yyyyyyy'


create_delta_table_from_catalog(spark, conformed.DataValidationDashboard, lake_descriptor)
dashboard_path = get_s3_path(conformed.DataValidationDashboard, lake_descriptor)
dashboard = DeltaTable.forPath(spark, dashboard_path)


# cast all to string
for c in cmt.columns:
    cmt = cmt.withColumn(c, col(c).cast(StringType()))

print(cmt.printSchema())

cmt_to_import = DynamicFrame.fromDF(cmt, glueContext, 'cmt_to_import')

glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=cmt_to_import,
    catalog_connection=rds_connection,
    connection_options={
        "database": rds_db_name,
        "dbtable": rds_db_table,
        "overwrite": "true",
    }
)
