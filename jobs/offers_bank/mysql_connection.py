from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, FloatType, StringType

from cvmdatalake import conformed, convert_columns_to_upper_case, ColumnSpec, create_delta_table_from_catalog
from cvmdatalake.security import get_secret
from cvmdatalake.transformations import mysql_connection



if __name__ == '__main__':

    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.transforms import *
    from awsglue.utils import getResolvedOptions

    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'lake_descriptor', 'cvm_environment',
                                         'rds_cvm_connection_name',
                                         'cvm_rds_db_name', 'rds_host',
                                         'cvm_offers_db_password_secret_name'])

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark: SparkSession = glueContext.spark_session
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

    # df = spark.read.jdbc(url=jdbc_url, table='cdp_stats', properties=connectionProperties)
    # df.show()
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
            # Create a new record
            table_to_truncate = "cdp_stats"
            sql = f"truncate table {table_to_truncate}"
            cursor.execute(sql)
        connection.commit()

    # column_stat.write.option("truncate", "true").jdbc(url=jdbc_url, table='cdp_stats', mode="overwrite",
    #                                          properties=connectionProperties);
    # column_values.write.option("truncate", "true").jdbc(url=jdbc_url, table='cdp_stats_values', mode="overwrite",
    #                                                   properties=connectionProperties);

    # column_values.write.mode("overwrite").jdbc(url=jdbc_url, table='cdp_stats_values', properties=connectionProperties);
    # column_stat.write.mode("overwrite").jdbc(url=jdbc_url, table='cdp_stats', properties=connectionProperties);
    #
