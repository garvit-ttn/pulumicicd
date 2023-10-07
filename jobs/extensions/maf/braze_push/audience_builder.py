import sys
from typing import Optional, NamedTuple

import sqlglot
from pyspark import SparkContext
from pyspark.sql import DataFrame, Column, SparkSession
from pyspark.sql.functions import (
    col, lit, struct, to_json, shuffle, array, flatten, array_distinct, slice,
    current_timestamp, current_date, format_string, explode, split, regexp_extract, concat, when, collect_set
)
from pyspark.sql.types import StringType, Row

from cvmdatalake import (
    landing, conformed, create_delta_table_from_catalog, get_s3_path
)
from cvmdatalake.conformed import Promotions
from cvmdatalake.spark_extensions import get_show_str, write_delta
from cvmdatalake.transformations.id_map import enrich_with_customer_src_id, SourceSystem
from cvmdatalake.transformations.promotions import get_eligible_promotions

total_offers_assigned_to_customer = 10


def get_promos_ids_from_offers_bank(aud_info: Row, offer_bank_offers: DataFrame) -> list[str]:
    offer_bank_offer_ids: str = aud_info[landing.Campaign.offer_concat.name]
    offer_bank_offer_ids: list[str] = str.split(offer_bank_offer_ids, '+')

    offer_names = (
        offer_bank_offers
        .filter(col(landing.Offer.id.name).cast(StringType()).isin(offer_bank_offer_ids))
        .select(landing.Offer.offer_name.name)
    )

    promo_ids = offer_names.select(regexp_extract(col(landing.Offer.offer_name.name), "\((\d+)\)", 1)).collect()
    return list(map(lambda x: x[0], promo_ids))


def overwrite_nbo_with_static_offers_for_audience(
        audience: DataFrame, aud_info: Row, promos: DataFrame, offer_bank_offers: DataFrame, id_map: DataFrame
) -> tuple[DataFrame, DataFrame]:
    print(f"Audience before mapping to braze: {audience.count()}")

    # Enrich audiences with braze id
    audience_with_ids = enrich_with_customer_src_id(
        input_df=audience, id_map=id_map,
        input_idi_gcr_col=conformed.CustomerProfile.idi_counterparty.name,
        output_src_col='braze_id', source=SourceSystem.Braze,
    )




    print(f"Audience after mapping to braze: {audience_with_ids.count()}")

    # Enrich audiences with share id
    audience_with_ids = enrich_with_customer_src_id(
        input_df=audience_with_ids, id_map=id_map,
        input_idi_gcr_col=conformed.CustomerProfile.idi_counterparty.name,
        output_src_col='share_id', source=SourceSystem.Share,
    )



    print(f"Audience after mapping to share: {audience_with_ids.count()}")

    # Rename customer id to gcr id
    audience_with_ids = audience_with_ids.withColumnRenamed(
        existing=conformed.CustomerProfile.idi_counterparty.name,
        new='gcr_id'
    )

    # Assign default offers based on eligibility criteria
    eligible_promos = get_eligible_promotions(promos)
    default_ids = list(map(lambda x: x[0], eligible_promos.select(Promotions.idi_offer.name).collect()))

    # Assign static offers based on audience
    offer_bank_offer_ids = get_promos_ids_from_offers_bank(aud_info, offer_bank_offers)
    print(f"Offers assigned to audience in UI {aud_info[landing.Campaign.name.name]}: {offer_bank_offer_ids}")

    number_of_default_offers = max(0, total_offers_assigned_to_customer - len(offer_bank_offer_ids))

    offer_assignment = (
        audience_with_ids
        .withColumn(
            'ui_offers',
            get_n_offers_at_random(
                offer_bank_offer_ids,
                min(len(offer_bank_offer_ids), total_offers_assigned_to_customer)
            )
        )
        .withColumn('default_offers', get_n_offers_at_random(default_ids, number_of_default_offers))
        .withColumn(Promotions.idi_offer.name, array_distinct(concat(col('ui_offers'), col('default_offers'))))
        .drop('ui_offers', 'default_offers')
    )

    offer_ids = offer_assignment.select(explode(col(Promotions.idi_offer.name))).dropDuplicates()
    offer_ids = list(map(lambda x: x[0], offer_ids.collect()))
    print(f"All offers assigned to audience {aud_info[landing.Campaign.name.name]}: {offer_ids}")

    offer_columns = [c.name for c in conformed.Promotions]
    static_offers = (
        promos
        .filter(col(conformed.Promotions.idi_offer.name).isin(offer_ids))
        .withColumn('details', to_json(struct(*offer_columns)))
        .select(Promotions.idi_offer.name, 'details')
    )

    print(
        f"Offers for audience {aud_info[landing.Campaign.name.name]} after mapping to promotions: {get_show_str(static_offers, truncate=False)}"
    )

    return offer_assignment, static_offers


def get_n_offers_at_random(offer_ids: list[str], n: int) -> Column:
    return slice(
        shuffle(array(*map(lit, offer_ids))),
        start=1, length=n
    )


class RunResult(NamedTuple):
    braze_audience: DataFrame
    static_nbo_for_braze: DataFrame
    static_nbo_for_share: DataFrame
    static_offer_to_upsert: DataFrame


def run(
        spark_session: SparkSession, aud_info: DataFrame,
        profiles: DataFrame, braze: DataFrame, campaigns: DataFrame, promos: DataFrame, offers: DataFrame,
        id_map: DataFrame
) -> RunResult:
    profiles.createTempView('profiles')
    profiles.cache()

    campaigns = campaigns.withColumn(
        landing.Campaign.audience_id.name,
        explode(split(landing.Campaign.audiences_concat.name, "\+"))
    )

    aud_info = aud_info.alias(landing.AudienceInfo.table_alias()).join(
        how="inner",
        other=campaigns.alias(landing.Campaign.table_alias()),
        on=col(landing.AudienceInfo.id.column_alias()) == col(landing.Campaign.audience_id.column_alias())
    )

    print(f"All audiences joined with campaigns: {get_show_str(aud_info)}")

    # sync only audiences with approved campaigns
    aud_info = aud_info.filter(col(landing.Campaign.active.column_alias())) \
        .filter(col(landing.Campaign.end_date.column_alias()) >= current_date()).filter(col(
        landing.Campaign.start_date.column_alias()) <= current_date())
    aud_info.show(1)

    print(f"All audiences with approved campaigns: {get_show_str(aud_info)}")

    aud_info = aud_info.select(
        landing.Campaign.name.column_alias(),
        landing.Campaign.offer_concat.column_alias(),
        landing.Campaign.start_date.column_alias(),
        landing.Campaign.end_date.column_alias(),
        landing.Campaign.touchpoints.column_alias(),
        landing.AudienceInfo.raw_query.column_alias(),
        landing.AudienceInfo.business_unit.column_alias(),
    ).collect()

    audiences: Optional[DataFrame] = None
    nbo_to_overwrite: Optional[DataFrame] = None
    static_offers_to_upsert_to_dynamodb: Optional[DataFrame] = None

    for ai in aud_info:
        # test_campaigns = [
        #     '20230620_UAE_SMBU_ExsellCVMPOCDecliners-MonitorTest_1_braze_197',
        # ]
        #
        # if ai[landing.Campaign.name.name] not in test_campaigns:
        #     print(f"skipping audience {ai[landing.Campaign.name.name]}")
        #     continue

        filter_query: str = ai[landing.AudienceInfo.raw_query.name]

        audience: DataFrame = (
            spark_session.sql(f"SELECT * FROM profiles WHERE {filter_query}")
            .select(
                conformed.CustomerProfile.idi_counterparty.name,
                conformed.CustomerProfile.cod_sor_counterparty.name
            )
            .dropDuplicates([
                conformed.CustomerProfile.idi_counterparty.name,
                conformed.CustomerProfile.cod_sor_counterparty.name
            ])
            .withColumn('audience', lit(ai[landing.Campaign.name.name]))
            .withColumn('business_unit', lit(ai[landing.AudienceInfo.business_unit.name]))
            .withColumn('campaign_end_date', lit(ai[landing.Campaign.end_date.name]))
        )
        # print("audience" , audience.show(truncate=False,n=150))

        nbo_for_audience, static_offers_for_audience = overwrite_nbo_with_static_offers_for_audience(
            audience=audience,
            aud_info=ai,
            promos=promos,
            offer_bank_offers=offers,
            id_map=id_map
        )

        # nbo_for_audience.show()
        nbo_to_overwrite = (
            nbo_to_overwrite.union(nbo_for_audience)
            if nbo_to_overwrite is not None else nbo_for_audience
        )

        static_offers_to_upsert_to_dynamodb = (
            static_offers_to_upsert_to_dynamodb.union(static_offers_for_audience)
            if static_offers_to_upsert_to_dynamodb is not None else static_offers_for_audience
        )

        # nbo_to_overwrite.show()

        print(f"audience {ai[landing.Campaign.name.name]} criteria: {filter_query}")
        print(f"audience {ai[landing.Campaign.name.name]} count: {audience.count()}")

        audiences = audience if audiences is None else audiences.union(audience)

    if audiences is None:
        raise Exception("There are no new audiences to push")

    print(f"In total we update audiences for {audiences.count()} customers in braze")

    print("audiences")
    audiences.show()
    braze = braze.select("external_id", "bu", "org_code", "gcr_id")

    nbo_to_overwrite.printSchema()
    nbo_to_overwrite.show(truncate=False)

    braze.printSchema()
    braze.show(truncate=False)

    mapped_to_braze_id: DataFrame = (
        nbo_to_overwrite.join(
            braze,
            (nbo_to_overwrite.braze_id == braze.external_id) & (nbo_to_overwrite.business_unit == braze.bu) & (
                    nbo_to_overwrite.gcr_id == braze.gcr_id),
            "inner"
        )
        .dropDuplicates(['gcr_id'])
    )

    # enforces Spark to perform computations and cache the results
    mapped_to_braze_id = mapped_to_braze_id.persist()

    braze_to_overwrite, share_to_overwrite = final_prep_for_nbo_to_overwrite(mapped_to_braze_id.select(
        braze.gcr_id, nbo_to_overwrite.braze_id, nbo_to_overwrite.share_id, nbo_to_overwrite.idi_offer
    ))

    mapped_to_braze_id_to_delta = (
        mapped_to_braze_id.select(
            braze.gcr_id, braze.external_id, nbo_to_overwrite.audience, nbo_to_overwrite.business_unit)
        .drop("gcr_id")
        .withColumn("ingestion_timestamp", current_timestamp())
    )

    print("final output to be added to delta table")
    mapped_to_braze_id_to_delta.show(truncate=False)
    print("mapped_to_braze_id count", mapped_to_braze_id_to_delta.count())

    discrepancies = find_discrepancies(mapped_to_braze_id_to_delta, 'external_id', braze_to_overwrite, 'braze_id')
    print(
        f"{discrepancies.count()} discrepancies for braze with nbo_to_overwrite  {get_show_str(discrepancies, truncate=False)}"
    )

    return RunResult(
        braze_audience=mapped_to_braze_id_to_delta,
        static_nbo_for_braze=braze_to_overwrite,
        static_nbo_for_share=share_to_overwrite,
        static_offer_to_upsert=static_offers_to_upsert_to_dynamodb.dropDuplicates([Promotions.idi_offer.name])
    )


def find_discrepancies(
        audiences: DataFrame, aud_braze_id_name: str,
        braze_offers: DataFrame, offers_braze_id_name: str,
) -> DataFrame:
    return audiences.alias('aud').join(
        how='left_anti', other=braze_offers.alias('braze'),
        on=col(f'aud.{aud_braze_id_name}') == col(f'braze.{offers_braze_id_name}')
    ).select(col(f'aud.{aud_braze_id_name}'))


def final_prep_for_nbo_to_overwrite(nbo_to_overwrite: DataFrame) -> tuple[DataFrame, DataFrame]:
    # print("before final", nbo_to_overwrite.show(truncate=False))
    final = (
        nbo_to_overwrite
        .groupBy('share_id', 'gcr_id', 'braze_id').agg(
            collect_set(col(Promotions.idi_offer.name)).alias('offers')
        )
        .withColumn(Promotions.idi_offer.name, array_distinct(flatten(col('offers'))))
        .withColumn("share_id", format_string("%s#share", col('share_id')))
        .withColumn("braze_id", format_string("%s#share", col('braze_id')))
        .select('share_id', 'braze_id', Promotions.idi_offer.name))
    # print("after final ", final.show(truncate=False))

    print(f"final nbo to overwrite schema before split: {final.printSchema()}")

    share_to_upsert = final.select('share_id', Promotions.idi_offer.name).dropDuplicates(['share_id'])
    braze_to_upsert = final.select('braze_id', Promotions.idi_offer.name).dropDuplicates(['braze_id'])

    return braze_to_upsert, share_to_upsert


if __name__ == '__main__':
    from awsglue import DynamicFrame
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from delta import DeltaTable

    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'lake_descriptor', 'cvm_environment',
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
    id_mapping = DeltaTable.forPath(spark, get_s3_path(conformed.IdMapping, lake_descriptor)).toDF()
# <<<<<<< HEAD
    # braze_attributes = DeltaTable.forPath(spark, get_s3_path(conformed.CustomAttributes, lake_descriptor)).toDF()
    braze_attributes = spark.read.parquet('s3://cvm-prod-landing-5d6c06b/bu_external_id/2023-07-04/')
# =======
    braze_attributes = DeltaTable.forPath(spark, get_s3_path(conformed.CustomAttributes, lake_descriptor)).toDF()
    # braze_attributes = spark.read.parquet('s3://cvm-prod-landing-5d6c06b/bu_external_id/2023-06-13/')
# >>>>>>> 13865df (rolled back prod version for dynamodb and audience builder job)
    braze_attributes = braze_attributes.withColumn("bu", when(braze_attributes.org_code == "LYL", "SHARE")
                                                   .when(braze_attributes.org_code == "FSN", "LIFESTYLE")
                                                   .when(braze_attributes.org_code == "CRF", "CRF")
                                                   .when(braze_attributes.org_code == "LNE", "LEC")
                                                   .when(braze_attributes.org_code == "VOX", "LEC")
                                                   .when(braze_attributes.org_code == "WFI", "SMBU")
                                                   .when(braze_attributes.org_code == "ECP", "SMBU")
                                                   .otherwise("null"))

    print(f"braze_attributes with empty bu: {braze_attributes.filter(col('bu').isNull()).count()}")

    promos_df = create_delta_table_from_catalog(spark, conformed.Promotions, lake_descriptor).toDF()
    id_map_df = create_delta_table_from_catalog(spark, conformed.IdMapping, lake_descriptor).toDF()

    offers_bank_offers: DataFrame = glueContext.create_dynamic_frame_from_options(
        connection_type='mysql',
        connection_options={
            "useConnectionProperties": "true",
            "dbtable": 'offerbank_offers',
            "connectionName": rds_connection,
        }
    ).toDF()

    audience_info: DataFrame = glueContext.create_dynamic_frame_from_options(
        connection_type='mysql',
        connection_options={
            "useConnectionProperties": "true",
            "dbtable": 'cdp_audience_information',
            "connectionName": rds_connection,
        }
    ).toDF()
    # audience_info.show(10)

    campaigns_df: DataFrame = glueContext.create_dynamic_frame_from_options(
        connection_type='mysql',
        connection_options={
            "useConnectionProperties": "true",
            "dbtable": 'offerbank_campaigns',
            "connectionName": rds_connection,
        }
    ).toDF()

    result = run(
        spark, audience_info, customer_profiles, braze_attributes, campaigns_df,
        promos_df, offers_bank_offers, id_map_df
    )

    print("printing run output for braze_audience")
    result.braze_audience.show(truncate=False)
    print(result.braze_audience.count())
    # processed_audience name to be updated in RDS table for setting it inactive
    processed_campaign: DataFrame = (
        result.braze_audience.dropDuplicates(["audience"]).withColumn("campaign_name", col('audience'))
        .withColumn('start_timestamp', current_timestamp())
        .withColumn('end_timestamp', current_timestamp()).select("campaign_name", "start_timestamp", "end_timestamp")
    )
    print("processed_campaign")
    print(f"Processed Status is found for below list of campaigns")
    processed_campaign.show(truncate=False)


    # Store the results as tables in Athena for future reference
    write_delta(
        result.braze_audience, conformed.Audiences, spark, lake_descriptor, save_mode="append"
    )

    write_delta(
        result.static_nbo_for_braze, conformed.AudiencesOfferBraze, spark, lake_descriptor, save_mode="overwrite"
    )

    write_delta(
        result.static_nbo_for_share, conformed.AudiencesOfferShare, spark, lake_descriptor, save_mode="overwrite"
    )

    # overwrite nbo with static offers
    glueContext.write_dynamic_frame.from_options(
        frame=DynamicFrame.fromDF(result.static_offer_to_upsert, glueContext, "static_offer_to_upsert"),
        connection_type="dynamodb",
        connection_options={
            "tableName": "cvm-offers",
            "overwrite": "true"
        }
    )

    # overwrite nbo for Braze with static offers
    glueContext.write_dynamic_frame.from_options(
        frame=DynamicFrame.fromDF(result.static_nbo_for_braze, glueContext, "static_nbo_for_braze"),
        connection_type="dynamodb",
        connection_options={
            "tableName": "cvm-braze",
            "overwrite": "true"
        }
    )

    # overwrite nbo for Share with static offers
    glueContext.write_dynamic_frame.from_options(
        frame=DynamicFrame.fromDF(result.static_nbo_for_share, glueContext, "static_nbo_for_share"),
        connection_type="dynamodb",
        connection_options={
            "tableName": "cvm-share",
            "overwrite": "true"
        }
    )

    processed_audience_current: DataFrame = glueContext.create_dynamic_frame_from_options(
        connection_type='mysql',
        connection_options={
            "useConnectionProperties": "true",
            "dbtable": 'campaign_processed',
            "connectionName": rds_connection,
        }
    ).toDF()

    processed_campaign_to_update = processed_campaign.alias('latest').join(
        how="left_anti", other=processed_audience_current.alias('current'),
        on=col("latest.campaign_name") == col("current.campaign_name")
    )

    processed_audience = DynamicFrame.fromDF(processed_campaign_to_update, glueContext, 'processed_audience')
    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=processed_audience,
        catalog_connection=rds_connection,
        connection_options={
            "database": rds_db_name,  # 'cvm_new',
            "dbtable": 'campaign_processed',
            "overwrite": "true",
        }
    )