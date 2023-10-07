from awsglue import DynamicFrame
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from delta.tables import DeltaTable
from pyspark.sql.functions import *
from pyspark.sql.types import StringType
from cvmdatalake.security import get_secret
from cvmdatalake import conformed, create_delta_table_from_catalog

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'lake_descriptor', 'cvm_environment', 'rds_cvm_connection_name',
                                     'cvm_rds_db_name','rds_host','cvm_offers_db_password_secret_name'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

rds_connection = args['rds_cvm_connection_name']
lake_descriptor = args['lake_descriptor']
environment = args['cvm_environment']
rds_db_name = args['cvm_rds_db_name']
rds_host = args['rds_host']
jdbcPort = 3306
username = 'backend'
rds_secret_name = args['cvm_offers_db_password_secret_name']
rds_password = get_secret(rds_secret_name)

print(rds_secret_name)
print(rds_host)
print(rds_password)

jdbc_url = "jdbc:mysql://{0}:{1}/{2}".format(rds_host, jdbcPort, rds_db_name)

connectionProperties = {
        "user": username,
        "password": rds_password
}


customer_profiles = create_delta_table_from_catalog(spark, conformed.CustomerProfile, lake_descriptor).toDF()
#customer_profiles = DeltaTable.forPath(spark, 's3://cvm-uat-conformed-d5b175d/customer_profile_improved/').toDF()

customer_profiles = (
    customer_profiles
    .drop(conformed.CustomerProfile.bu.name)
    .dropDuplicates([conformed.CustomerProfile.idi_counterparty.name])
    .withColumn('account_id', lit(1).cast(StringType()))
    .withColumn('id', col(conformed.CustomerProfile.idi_counterparty.name).cast(StringType()))
)

# cast all to string
for c in customer_profiles.columns:
    customer_profiles = customer_profiles.withColumn(c, col(c).cast(StringType()))

print(customer_profiles.printSchema())
print(customer_profiles.count())
# customer_profiles_to_import = DynamicFrame.fromDF(customer_profiles, glueContext, 'customer_profiles_to_import')
print("Truncate Started !")
import pymysql
import pymysql.cursors

# Connect to the database using db details or fetch these from Glue connections
connection = pymysql.connect(host=rds_host,
                                user=username,
                                password=rds_password,
                                database=rds_db_name,
                                cursorclass=pymysql.cursors.DictCursor)
with connection:
    with connection.cursor() as cursor:
        table_to_truncate = "cdp_results"
        sql = f"delete from {table_to_truncate}"
        cursor.execute(sql)
    connection.commit()

print("Truncate Completed !")
print("Write Started !")
customer_profiles.write.mode("append").jdbc(url=jdbc_url, table='cdp_results', properties=connectionProperties);
print(" Write Ended !")


# glueContext.write_dynamic_frame.from_jdbc_conf(
#     frame=customer_profiles_to_import,
#     catalog_connection=rds_connection,
#     connection_options={
#         "database": rds_db_name,
#         "dbtable": 'cdp_results',
#         "overwrite": "true",
#     }
# )
