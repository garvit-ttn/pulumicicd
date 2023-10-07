import sys
from typing import Optional, NamedTuple

from pyspark import SparkContext
from pyspark.sql import DataFrame, Column, SparkSession
from pyspark.sql.functions import (
    col, lit, struct, to_json, shuffle, array, flatten, array_distinct, slice,
    current_timestamp, current_date, format_string, explode, split, regexp_extract, concat, when, collect_set, to_date
)
from pyspark.sql.types import StringType, Row
from cvmdatalake import conformed, create_delta_table_from_catalog, creat_delta_table_if_not_exists, get_s3_path


class RunResult(NamedTuple):
    audiences_to_update: DataFrame


def refresh_audience_details(spark_session: SparkSession, audiences: DataFrame, campaigns: DataFrame) -> RunResult:
    audiences.createTempView('audiences')
    audiences.cache()

    # campaigns.show()
    # print("count :", campaigns.count())
    campaigns = campaigns.filter((col('status') == 'inactive') | (col('status') == 'draft')).filter(
        col('end_date') >= current_date())
    # campaigns.show()
    # print("count :", campaigns.count())

    campaigns = campaigns.select('name')
    campaigns.show(truncate=False)
    print("count :", campaigns.count())

    campaigns = campaigns.rdd.flatMap(lambda x: x).collect()
    print(campaigns)

    audiences_update = audiences.filter(col('audience').cast(StringType()).isin(campaigns))
    print("records",audiences.count())
    print("records to update",audiences_update.count())

    # audiences_to_update = audiences.filter(col('audience').like("%GeofenceMOEPropensity_1_braze_225%"))
    # audiences_to_update1 = audiences.filter(col('audience').like("%20230706_UAE_SHARE_MonoToCrossC4_1_braze_223%"))
    # audiences_to_update2 = audiences.filter(col('audience').like("%20230713_UAE_SHARE_FAB0013 Carrefour Missed Opportunity_1_braze_226%"))
    # audiences_to_update3 = audiences.filter(col('audience').like("%20230601_UAE_SHARE_cadence-test-static-audience5-voxquarterly_1_braze_147%"))
    # audiences_to_update4 = audiences.filter(col('audience').like("%20230713_UAE_SHARE_FAB0014 Missed Opportunity VOX_1_braze_228%"))
    audiences_to_update5 = audiences.filter(col('audience').like("%20230824_UAE_CRF_Test-Audience-Push_1_braze_246%"))

    # audience = audiences_to_update2.union(audiences_to_update1)
    # audience1 = audience.union(audiences_to_update3)
    # audience2 = audience1.union(audiences_to_update4)
    # audience3 = audience2.union(audiences_update)



    # audiences_to_display = audiences_to_update.groupBy(col('audience')).count().alias('count')
    # audiences_to_display.show(truncate=False)

    # audiences_to_update = audiences_to_update.filter(col('campaign_end_date') >= current_date())\
    #     .withColumn('campaign_end_date', to_date(lit("31-08-2023"),"dd-MM-yyyy"))
    # audiences.show()
    # \

        # .filter(
        # col('audience').like("%20230706_UAE_SHARE_MonoToCrossC4_1_braze_223%"))\
        # .filter(col('audience').like('%20230713_UAE_SHARE_FAB0013 Carrefour Missed Opportunity_1_braze_226%'))

    print("records to update", audiences_to_update5.count())
    audiences_to_update = audiences_to_update5.withColumn('campaign_end_date', to_date(lit("31-08-2023"), "dd-MM-yyyy"))

    print("records to update", audiences_to_update.count())

    return RunResult(
        audiences_to_update=audiences_to_update,
    )


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
    creat_delta_table_if_not_exists(spark, conformed.AudiencesBraze, lake_descriptor)
    audiencesbraze_path = get_s3_path(conformed.AudiencesBraze, lake_descriptor)
    audiencesbrazepush = DeltaTable.forPath(spark, audiencesbraze_path)
    audiences_df = create_delta_table_from_catalog(spark, conformed.AudiencesBraze, lake_descriptor).toDF()
    audiences_df.printSchema()
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
    campaigns_df: DataFrame = glueContext.create_dynamic_frame_from_options(
        connection_type='mysql',
        connection_options={
            "useConnectionProperties": "true",
            "dbtable": 'offerbank_campaigns',
            "connectionName": rds_connection,
        }
    ).toDF()

    result = refresh_audience_details(spark, audiences_df, campaigns_df)
    audiencesbrazepush.alias(conformed.AudiencesBraze.table_alias()).merge(
        result.audiences_to_update.alias('braze_audience'),
        f"""
            braze_audience.external_id = {conformed.AudiencesBraze.external_id.column_alias()} and
            braze_audience.gcr_id = {conformed.AudiencesBraze.gcr_id.column_alias()} and
            braze_audience.share_id = {conformed.AudiencesBraze.share_id.column_alias()} and
            braze_audience.campaign_type = {conformed.AudiencesBraze.campaign_type.column_alias()} and
            braze_audience.active_appgroup = {conformed.AudiencesBraze.active_appgroup.column_alias()} and
            braze_audience.business_unit = {conformed.AudiencesBraze.business_unit.column_alias()} and
            braze_audience.audience = {conformed.AudiencesBraze.audience.column_alias()} and
            braze_audience.ingestion_timestamp = {conformed.AudiencesBraze.ingestion_timestamp.column_alias()}
            """
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
