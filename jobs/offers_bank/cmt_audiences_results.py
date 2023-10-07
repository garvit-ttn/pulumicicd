from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from typing import NamedTuple
# from cvmdatalake import (conformed, create_delta_table_from_catalog, landing, get_s3_path)
import sqlglot
from cvmdatalake import conformed, create_delta_table_from_catalog, landing, creat_delta_table_if_not_exists, \
    get_s3_path
from cvmdatalake.spark_extensions import get_show_str, write_delta


class RunResult(NamedTuple):
    profiles: DataFrame


def cmt_data_prep_details(spark_session: SparkSession, profiles: DataFrame, aud_info: DataFrame, campaigns: DataFrame) \
        -> RunResult:
    profiles.createTempView('profiles')
    profiles.cache()

    audiences: Optional[DataFrame] = None
    aud_info = aud_info.alias(landing.AudienceInfo.table_alias())
    # print(aud_info)
    campaigns = campaigns.withColumn(
        landing.Campaign.audience_id.name,
        explode(split(landing.Campaign.audiences_concat.name, "\+"))
    )

    aud_info = aud_info.alias(landing.AudienceInfo.table_alias()).join(
        how="inner",
        other=campaigns.alias(landing.Campaign.table_alias()),
        on=col(landing.AudienceInfo.id.column_alias()) == col(landing.Campaign.audience_id.column_alias())
    )

    aud_info = aud_info.filter(col(landing.Campaign.name.column_alias()).like("%Exsell%"))
    aud_info.show(truncate=False)
    aud_info.printSchema()

    # aud_info = aud_info.join(filter_campaigns,aud_info.id == filter_campaigns.audience_concat,"inner")
    # aud_info.show(1)

    aud_info = aud_info.select(
        landing.AudienceInfo.campaign_name.column_alias(),
        landing.Campaign.name.column_alias(),
        landing.AudienceInfo.raw_query.column_alias(),
    ).collect()
    print(aud_info)

    for ai in aud_info:
        # test_campaigns = [
        #     '20230417_UAE_SHARE_SHAREUC1CRFXBU_1',
        # ]

        # if ai[landing.AudienceInfo.campaign_name.name] not in test_campaigns:
        #     print(f"skipping audience {ai[landing.AudienceInfo.campaign_name.name]}")
        #     continue

        filter_query: str = ai[landing.AudienceInfo.raw_query.name]

        result = sqlglot.transpile(
            sql=(f"SELECT * FROM profiles WHERE {filter_query}"),
            read=sqlglot.Dialects.MYSQL,
            write=sqlglot.Dialects.SPARK2
        )

        # print("profile_result :", result[0])

        audience: DataFrame = (
            spark_session.sql(f"{result[0]}")
            .withColumn('cvm_audience', lit(ai[landing.AudienceInfo.campaign_name.name]))
            .withColumn('campaign_name', lit(ai[landing.Campaign.name.name]))
            .withColumn("ingestion_timestamp", current_timestamp()))
        # .withColumn('idi_counterparty', lit(ai[landing.AudienceInfo.idi_counterparty.name])))

        # audience.printSchema()
        # audience.show(truncate=False)
        audience = audience.select(col("cvm_audience").alias('cvm_audience'),
                                   col("idi_counterparty").alias('idi_counterparty'),
                                   col("campaign_name").alias('campaign_name'),
                                   col("ingestion_timestamp").alias('ingestion_timestamp'))
        # audience.printSchema()
        # audience.show()

        print(f"cvm_audience {ai[landing.AudienceInfo.campaign_name.name]} criteria: {filter_query}")

        audiences = audience if audiences is None else audiences.union(audience)

    if audiences is None:
        raise Exception("There are no new audiences to push")

    print(f"Total  {audiences.count()} customers to be updated")
    # audiences.show(truncate=False)

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
    # customer_profiles.show(n=5)

    audience_info: DataFrame = glueContext.create_dynamic_frame_from_options(
        connection_type='mysql',
        connection_options={
            "database": rds_db_name,
            "useConnectionProperties": "true",
            "dbtable": 'cdp_audience_information',
            "connectionName": rds_connection,
        }
    ).toDF()

    offerbank_campaigns: DataFrame = glueContext.create_dynamic_frame_from_options(
        connection_type='mysql',
        connection_options={
            "database": rds_db_name,
            "useConnectionProperties": "true",
            "dbtable": 'offerbank_campaigns',
            "connectionName": rds_connection,
        }
    ).toDF()

    # audience_info.show(n=5)

    cmt_data_prep = cmt_data_prep_details(spark, customer_profiles, audience_info, offerbank_campaigns)
    print(cmt_data_prep.profiles.show(truncate=False))

    repartitioned_cmt_data_prep = cmt_data_prep.profiles.repartition((int(cmt_data_prep.profiles.count() / 100000)))

    creat_delta_table_if_not_exists(spark, conformed.AudienceCmtdata, lake_descriptor)
    delta_path = get_s3_path(conformed.AudienceCmtdata, lake_descriptor)
    repartitioned_cmt_data_prep.write.format('delta').option("overwriteSchema", 'true').mode("overwrite").save(
        delta_path)
