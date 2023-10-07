from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from typing import NamedTuple
from cvmdatalake import conformed, create_delta_table_from_catalog, landing
import sqlglot



class RunResult(NamedTuple):
    profiles: DataFrame


def refresh_audience_details(spark_session: SparkSession, profiles: DataFrame, aud_info: DataFrame) \
        -> RunResult:
    profiles.createTempView('profiles')
    profiles.cache()

    audiences: Optional[DataFrame] = None
    aud_info = aud_info.alias(landing.AudienceInfo.table_alias())

    aud_info.show(1)
    aud_info = aud_info.filter(col('campaign_type') == 'dynamic')
    print("Total audiences to be updated with dynamic campaign_type", aud_info.count())
    aud_info = aud_info.select(
        landing.AudienceInfo.campaign_name.column_alias(),
        landing.AudienceInfo.campaign_type.column_alias(),
        landing.AudienceInfo.raw_query.column_alias(),
        landing.AudienceInfo.business_unit.column_alias(),
        landing.AudienceInfo.audience_count.column_alias(),
        landing.AudienceInfo.uplift_clv.column_alias(),
    ).collect()
    # print(aud_info)

    for ai in aud_info:
        # test_campaigns = [
        #     '20230615_UAE_SHARE_Ishali Dummy Audience_1',
        # ]
        #
        # if ai[landing.AudienceInfo.campaign_name.name] not in test_campaigns:
        #     print(f"skipping audience {ai[landing.AudienceInfo.campaign_name.name]}")
        #     continue

        print(f"audience {ai[landing.AudienceInfo.campaign_name.name]}")
        filter_query: str = ai[landing.AudienceInfo.raw_query.name]

        result = sqlglot.transpile(
            sql=(f"SELECT sum(clv_3m) as clv_count ,count(*) as count FROM profiles WHERE {filter_query}"),
            read=sqlglot.Dialects.MYSQL,
            write=sqlglot.Dialects.SPARK2
        )

        print(result[0])

        audience: DataFrame = (
            spark_session.sql(f"{result[0]}")
            .withColumn('campaign_name', lit(ai[landing.AudienceInfo.campaign_name.name]))
            .withColumn('business_unit', lit(ai[landing.AudienceInfo.business_unit.name]))
            .withColumn('audience_count', lit(ai[landing.AudienceInfo.audience_count.name]))
            .withColumn('uplift_clv', lit(ai[landing.AudienceInfo.uplift_clv.name])))

        # audience.printSchema()
        # audience.show(truncate=False)
        audience = audience.select(col("campaign_name").alias('audience_name'),
                                   col("clv_count").alias('uplift_clv'),
                                   col("count").alias('audience_count')
                                   )
        # audience.printSchema()
        # audience.show()

        print(f"audience {ai[landing.AudienceInfo.campaign_name.name]} criteria: {filter_query}")


        audiences = audience if audiences is None else audiences.union(audience)
    if audiences is None:
        raise Exception("There are no new audiences to push")
    print(f"Total  {audiences.count()} customers to be updated")
    audiences.show(truncate=False)

    return RunResult(
        profiles=audiences)


if __name__ == '__main__':
    from awsglue import DynamicFrame
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from delta import DeltaTable

    args = getResolvedOptions(sys.argv, [
        'JOB_NAME', 'lake_descriptor', 'cvm_environment',
        'rds_cvm_connection_name', 'cvm_rds_db_name'
    ])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    spark = glueContext.spark_session

    lake_descriptor = args['lake_descriptor']
    environment = args['cvm_environment']
    rds_connection = args['rds_cvm_connection_name']
    rds_db_name = args['cvm_rds_db_name']

    customer_profiles = create_delta_table_from_catalog(spark, conformed.CustomerProfile, lake_descriptor).toDF()

    audience_info: DataFrame = glueContext.create_dynamic_frame_from_options(
        connection_type='mysql',
        connection_options={
            "database": rds_db_name,
            "useConnectionProperties": "true",
            "dbtable": 'cdp_audience_information',
            "connectionName": rds_connection,
        }
    ).toDF()
    # audience_info.show()

    updated_audience_info = refresh_audience_details(spark, customer_profiles, audience_info)
    print(updated_audience_info.profiles.show())

    updated_audience_info = DynamicFrame.fromDF(updated_audience_info.profiles, glueContext, 'updated_audience_info')
    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=updated_audience_info,
        catalog_connection=rds_connection,
        connection_options={
            "database": rds_db_name,  # 'cvm_new',
            "dbtable": 'cdp_audienceclvcount',
            "overwrite": "true",
        }
    )

