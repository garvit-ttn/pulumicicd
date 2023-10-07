from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, FloatType, StringType

from cvmdatalake import conformed, convert_columns_to_upper_case, ColumnSpec, create_delta_table_from_catalog
from cvmdatalake.security import get_secret
from cvmdatalake.transformations import mysql_connection


columns_that_must_be_lower_case = [
    'f_rank_norm_proposition_level_3',
    'frequency_proposition_level_3',
    'customer_segment_proposition_level_3',
    'monetary_proposition_level_3',
    'm_rank_norm_proposition_level_3',
    'recency_proposition_level_3',
    'r_rank_norm_proposition_level_3',
]
def compute_cdp_results_stats(s: SparkSession, profiles: DataFrame) -> Tuple[DataFrame, DataFrame]:
    values: Optional[DataFrame] = None
    stats: List[Dict[str, Any]] = []

    for i, col_name in enumerate(profiles.columns):
        column_spec = cast(ColumnSpec, conformed.CustomerProfile[col_name])
        column_type = column_spec.data_type

        col_values: DataFrame = profiles.select(col_name).groupby(col_name).count()
        col_values = (
            col_values.sort(desc('count')).limit(50)
            .withColumnRenamed(col_name, 'label')
            .withColumnRenamed('count', 'value')
            .withColumn('label', col('label').cast(StringType()))
            .withColumn('stats_id', lit(i).cast(IntegerType()))
        )

        values = values.union(col_values) if values is not None else col_values

        col_stat = {
            'id': i,
            'column_name': col_name.upper() if col_name not in columns_that_must_be_lower_case else col_name,
            'min_value': 0.0, 'max_value': 0.0, 'average': 0.0, 'std_dev': 0.0, 'total_count': 0
        }

        profiles_stats = profiles

        if column_type not in ['string', 'date']:
            df_column_stats = profiles_stats.select(
                min(col_name).cast(FloatType()),
                max(col_name).cast(FloatType()),
                avg(col_name).cast(FloatType()),
                stddev(col_name).cast(FloatType()),
                count_distinct(col_name)
            ).collect()[0]

            col_stat['min_value'] = df_column_stats[0]
            col_stat['max_value'] = df_column_stats[1]
            col_stat['average'] = df_column_stats[2]
            col_stat['std_dev'] = df_column_stats[3]
            col_stat['total_count'] = df_column_stats[4]
        else:
            df_column_stats = profiles_stats.select(count_distinct(col_name)).collect()[0]
            col_stat['total_count'] = df_column_stats[0]

        stats.append(col_stat)

    stats_df = convert_columns_to_upper_case(s.createDataFrame(stats))
    stats_df = stats_df.withColumnRenamed('ID', 'id')

    values_df = convert_columns_to_upper_case(values)
    values_df = values_df.withColumn('id', monotonically_increasing_id())

    return values_df, stats_df


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
    customer_profiles = create_delta_table_from_catalog(spark, conformed.CustomerProfile, lake_descriptor)
    #customer_profiles = DeltaTable.forPath(spark, 's3://cvm-uat-conformed-d5b175d/customer_profile_improved/')

    column_values, column_stat = compute_cdp_results_stats(spark, customer_profiles.toDF())

    print(column_values.count())
    print(column_stat.count())
    print("Truncate & Loaded cdp_stats_values Started !")
    column_values.write.mode("overwrite")\
        .jdbc(url=jdbc_url, table='cdp_stats_values', properties=connectionProperties);
    print("cdp_stats_values Completed !")
    print("Truncate & Loaded cdp_stats Started !")
    column_stat.write.mode("overwrite")\
        .jdbc(url=jdbc_url, table='cdp_stats', properties=connectionProperties);
    print("cdp_stats Completed !")


