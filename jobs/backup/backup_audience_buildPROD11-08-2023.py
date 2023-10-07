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
from cvmdatalake import conformed, create_delta_table_from_catalog, creat_delta_table_if_not_exists, get_s3_path

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


# def enrich_id_mapping_external_id(audience: DataFrame, id_map: DataFrame):
#
#     return audience_with_ids

def overwrite_nbo_with_static_offers_for_audience(
        audience: DataFrame, aud_info: Row, promos: DataFrame, offer_bank_offers: DataFrame, id_map: DataFrame
) -> tuple[DataFrame, DataFrame]:
    # Enrich audiences with braze id
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


def final_prep_for_nbo_to_overwrite(nbo_to_overwrite: DataFrame) -> tuple[DataFrame, DataFrame]:
    print("before final", nbo_to_overwrite.show(truncate=False))
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


def find_discrepancies(
        audiences: DataFrame, aud_braze_id_name: str,
        braze_offers: DataFrame, offers_braze_id_name: str,
) -> DataFrame:
    return audiences.alias('aud').join(
        how='left_anti', other=braze_offers.alias('braze'),
        on=col(f'aud.{aud_braze_id_name}') == col(f'braze.{offers_braze_id_name}')
    ).select(col(f'aud.{aud_braze_id_name}'))


def delta_audience(audiences_df: DataFrame, delta_df: DataFrame, campaign_name: str):
    print("Delta left anti join loop for audiences ")
    audiences_df = audiences_df.filter(col('audience') == campaign_name)
    print(campaign_name)
    audiences_df.show(truncate=False)
    delta_df.show(truncate=False)
    delta_audience_ids = audiences_df.join(delta_df,
                                           (audiences_df.audience == delta_df.audience) & (
                                                   delta_df.gcr_id == audiences_df.idi_counterparty),
                                           "left_anti")
    delta_audience_ids.show(truncate=False)
    delta_audience_ids.printSchema()
    delta_audience_ids.count()
    return delta_audience_ids


def braze_join(braze, audience_with_ids):
    braze = braze.select("external_id", "bu", "org_code", "gcr_id")
    mapped_to_braze_id: DataFrame = (
        audience_with_ids.join(
            braze,
            (audience_with_ids.braze_id == braze.external_id) & (audience_with_ids.business_unit == braze.bu) & (
                    audience_with_ids.gcr_id == braze.gcr_id),
            "inner"
        )
        .dropDuplicates(['gcr_id', 'audience'])
    )

    print(f"Audience after mapping to bu_external_id: {mapped_to_braze_id.count()}")

    return mapped_to_braze_id


class RunResult(NamedTuple):
    braze_audience: DataFrame
    delta_audience: DataFrame
    processed_audience: DataFrame
    static_nbo_for_braze: DataFrame
    static_nbo_for_share: DataFrame
    static_offer_to_upsert: DataFrame


def braze_audience_builder(
        spark_session: SparkSession, aud_info: DataFrame,
        profiles: DataFrame, braze: DataFrame, campaigns: DataFrame,
        id_map: DataFrame, promos: DataFrame, offers: DataFrame, delta_df: DataFrame

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
    aud_info.count()

    campaign_display = aud_info.select('campaign_name').distinct()

    print(f"All audiences with approved campaigns: {campaign_display.show(truncate=False)}")
    delta_mapping = delta_df.select('audience', 'external_id', 'campaign_end_date').distinct()
    delta_mapping.show(truncate=False)
    print("Delta table has audiences count :", delta_mapping.count())

    aud_info = aud_info.select(
        landing.Campaign.name.column_alias(),
        landing.Campaign.offer_concat.column_alias(),
        landing.AudienceInfo.campaign_type.column_alias(),
        landing.Campaign.start_date.column_alias(),
        landing.Campaign.end_date.column_alias(),
        landing.Campaign.touchpoints.column_alias(),
        landing.AudienceInfo.raw_query.column_alias(),
        landing.AudienceInfo.business_unit.column_alias(),
        # landing.Campaign.created_at.column_alias(),
    ).collect()

    audiences: Optional[DataFrame] = None
    processed_audiences: Optional[DataFrame] = None
    deltaaudience_ids: Optional[DataFrame] = None
    static_offers_to_upsert_to_dynamodb: Optional[DataFrame] = None

    for ai in aud_info:

        # test_campaigns = [

        #     '20230719_UAE_SMBU_ishaliDELTASMBU_1_braze_320',
        # ]
        # if ai[landing.Campaign.name.name] not in test_campaigns:
        #     print(f"skipping audience {ai[landing.Campaign.name.name]}")
        #     continue

        filter_query: str = ai[landing.AudienceInfo.raw_query.name]
        result = sqlglot.transpile(
            sql=(f"SELECT * FROM profiles WHERE {filter_query}"),
            read=sqlglot.Dialects.MYSQL,
            write=sqlglot.Dialects.SPARK2
        )

        print(result[0])

        audience: DataFrame = (
            spark_session.sql(f"{result[0]}")
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
            .withColumn('campaign_start_date', lit(ai[landing.Campaign.start_date.name]))
            .withColumn('campaign_end_date', lit(ai[landing.Campaign.end_date.name]))
            .withColumn('campaign_type', lit(ai[landing.AudienceInfo.campaign_type.name]))
            # .withColumn('start_timestamp', lit(ai[landing.Campaign.created_at.name]))
        )

        delta_distinct_audience = delta_mapping.select("audience").distinct()
        print(type(delta_mapping))

        audience_count = delta_mapping.filter(col('audience') == ai[landing.Campaign.name.name]).count()
        audience_end_date = delta_mapping \
            .filter(col('audience') == ai[landing.Campaign.name.name]) \
            .withColumn("active_end_date", when(delta_mapping.campaign_end_date > current_date(), lit(0))
                        .otherwise(lit(1))).select('audience', 'campaign_end_date', 'active_end_date').distinct()

        print(audience_end_date.show())

        # if delta_mapping is None:
        #     print("Scenario 1 : This campaign is a fresh run, Hence No Delta records to be fetched")
        #     audience_with_ids, static_offers_for_audience = overwrite_nbo_with_static_offers_for_audience(
        #         audience=audience,
        #         aud_info=ai,
        #         promos=promos,
        #         offer_bank_offers=offers,
        #         id_map=id_map
        #     )
        #     audience_with_ids.show()
        # # elif ai[landing.Campaign.name.name]) not in delta_distinct_audience:
        # elif
        if audience_count == 0:
            print(f"audience {ai[landing.Campaign.name.name]}")
            print("elif-if starts here")
            # delta_mapping = delta_mapping.collect()
            print("Scenario 2 This campaign is a fresh run, Hence No Delta records to be fetched")
            print(ai)
            audience.show(truncate=False)
            audience_with_ids, static_offers_for_audience = overwrite_nbo_with_static_offers_for_audience(
                audience=audience,
                aud_info=ai,
                promos=promos,
                offer_bank_offers=offers,
                id_map=id_map
            )
            mapped_to_braze_id = braze_join(braze, audience_with_ids)
            processed_audience: DataFrame = (
                mapped_to_braze_id.groupBy(col('audience')).count().alias('processed_records')
                .withColumn("exsell_records", lit(audience.count()))
            )
            processed_audience = processed_audience.withColumnRenamed("count", "processed_records")

            # enforces Spark to perform computations and cache the results
            mapped_to_braze_id = mapped_to_braze_id.persist()

            print(f"audience {ai[landing.Campaign.name.name]} criteria: {filter_query}")
            print(f"audience {ai[landing.Campaign.name.name]} count: {mapped_to_braze_id.count()}")
        else:
            print("Scenario 3 This campaign has been run earlier, Hence Delta records to be fetched")
            print("campaign_end_date is greater hence check for delta records")
            # delta_table_ids = delta_audience(audience, delta_df, delta_mapping['campaign_name'])
            print(ai[landing.Campaign.end_date.name])
            # end_date = delta_mapping.filter(col(audience) == ai[landing.Campaign.name.name]).select('campaign_end_date').distinct()
            # end_date.show(truncate=False)
            audience.show(truncate=False)
            print(current_date())

            audience_end_date.show()

            # if audience_end_date.filter(col('active_end_date') == 0):
            delta_table_ids = delta_audience(audience, delta_df, ai[landing.Campaign.name.name])
            print(f"Audience after mapping to DeltaMapping Table: {delta_table_ids.count()}")

            audience_with_ids, static_offers_for_audience = overwrite_nbo_with_static_offers_for_audience(
                audience=delta_table_ids,
                aud_info=ai,
                promos=promos,
                offer_bank_offers=offers,
                id_map=id_map
            )
            mapped_to_braze_id = braze_join(braze, audience_with_ids)
            processed_audience: DataFrame = (
                mapped_to_braze_id.groupBy(col('audience')).count().alias('processed_records')
                .withColumn("exsell_records", lit(audience.count()))
            )
            processed_audience = processed_audience.withColumnRenamed("count", "processed_records")

            # enforces Spark to perform computations and cache the results
            mapped_to_braze_id = mapped_to_braze_id.persist()

            print(f"audience {ai[landing.Campaign.name.name]} criteria: {filter_query}")
            print(f"audience {ai[landing.Campaign.name.name]} count: {mapped_to_braze_id.count()}")

            # deltaaudience_ids = delta_table_ids if deltaaudience_ids is None else deltaaudience_ids.union(
            #     delta_table_ids)
            # deltaaudience_ids.show(truncate=False)
            # deltaaudience_ids.count()

            # if deltaaudience_ids is None:
            #     raise Exception("There are no delta audiences to push")

            # else:
            #     print("Scenario 4 ")
            #     print("campaign_end_date is greater hence check for delta records")
            #     audience_with_ids, static_offers_for_audience = overwrite_nbo_with_static_offers_for_audience(
            #         audience=audience,
            #         aud_info=ai,
            #         promos=promos,
            #         offer_bank_offers=offers,
            #         id_map=id_map
            #     )
            #     audience_with_ids.show()

        audience_with_ids.count()

        # audiences = mapped_to_braze_id if audiences is None else audiences.union(mapped_to_braze_id)


        static_offers_to_upsert_to_dynamodb = (
            static_offers_to_upsert_to_dynamodb.union(static_offers_for_audience)
            if static_offers_to_upsert_to_dynamodb is not None else static_offers_for_audience
        )

        processed_audiences = processed_audience if processed_audiences is None else processed_audiences.union(
            processed_audience)
        audiences = mapped_to_braze_id if audiences is None else audiences.union(mapped_to_braze_id)

    if audiences is None:
        raise Exception("There are no new audiences to push")

    print(f"In total we update audiences for {audiences.count()} customers in braze")
    audiences.show(2, truncate=False)
    processed_audiences.show(2, truncate=False)

    braze_to_overwrite, share_to_overwrite = final_prep_for_nbo_to_overwrite(audiences.select(
        braze.gcr_id, audiences.braze_id, audiences.share_id, audiences.idi_offer
    ))

    mapped_to_braze_id_to_delta = (
        audiences
        .select(
            braze.gcr_id, braze.external_id, audiences.share_id, audiences.audience, audiences.business_unit,
            audiences.campaign_start_date, audiences.campaign_end_date, audiences.campaign_type)
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("active_appgroup", lit(True))
    )

    display = mapped_to_braze_id_to_delta.select('audience', 'campaign_end_date', 'campaign_type').distinct()
    display.show(truncate=False)

    delta_result = mapped_to_braze_id_to_delta.filter(col('campaign_end_date') > current_date()).filter(
        col('campaign_type') == 'dynamic')

    print("Delta mapping entry to be audiences athena table")
    delta_result.show(truncate=False)

    print("final output to be audiences athena table")
    mapped_to_braze_id_to_delta.show(truncate=False)
    print("audiences count", mapped_to_braze_id_to_delta.count())
    discrepancies = find_discrepancies(mapped_to_braze_id_to_delta, 'external_id', braze_to_overwrite, 'braze_id')
    print(
        f"{discrepancies.count()} discrepancies for braze with nbo_to_overwrite  {get_show_str(discrepancies, truncate=False)}"
    )

    return RunResult(
        braze_audience=mapped_to_braze_id_to_delta,
        delta_audience=delta_result,
        processed_audience=processed_audiences,
        static_nbo_for_braze=braze_to_overwrite,
        static_nbo_for_share=share_to_overwrite,
        static_offer_to_upsert=static_offers_to_upsert_to_dynamodb.dropDuplicates([Promotions.idi_offer.name])
    )


# def inactive_campaign_check(true_df,campaigns_df):
#
#     true_df.show(1,truncate=False)
#     campaigns_df.show(1,truncate=False)
#     true_df = true_df.join(campaigns_df,true_df.audience == campaigns_df.name,"inner")
#
#     # de-sync audiences with inactive campaigns
#     true_df = true_df\
#         .filter(col(landing.Campaign.active.column_alias()) == lit(0)) \
#         .filter(col(landing.Campaign.draft.column_alias() == lit(0)))\
#         .filter(col(landing.Campaign.processed.column_alias() == lit(0)))\
#         .filter(col(landing.Campaign.pending.column_alias() == lit(0)))
#
#     true_df = true_df.withColumn(
#         "active_appgroup", lit(False))
#
#     return true_df




def audience_push_to_braze(spark_session: SparkSession, audiences: DataFrame, audiencesbraze_df: DataFrame,
                           delta_df: DataFrame , campaigns_df: DataFrame):


    print("audiencesbraze_df",audiencesbraze_df.count())

    #bu join for per app group funcationality
    bu_info = audiences_df.select("business_unit").distinct()
    bu_join: DataFrame = (
        audiencesbraze_df.join(bu_info, audiencesbraze_df.business_unit == bu_info.business_unit, "inner")).withColumn(
        "active_appgroup", lit(True)).drop(bu_info.business_unit)
    print("bu_join",bu_join.count())

    main_df = bu_join
    main_df = main_df.dropDuplicates(['external_id', 'gcr_id','share_id','audience', 'business_unit'])

    # dynamic_df = main_df.join(delta_df,
    #                                   (main_df.audience == delta_df.audience) & (
    #                                           delta_df.external_id == main_df.external_id),
    #                                   "left_anti")
    #
    # static_df = dynamic_df.join(main_df,
    #                           (main_df.audience == delta_df.audience) & (
    #                                   delta_df.external_id == main_df.external_id),
    #                           "inner")
    #
    # print("dynamic_df",dynamic_df.count())
    # print("static_df", static_df.count())

    campaign_distinct = main_df.select('audience').distinct().collect()
    print(campaign_distinct)
    print("main_df :  ", main_df.count())

    main_df.show(1,truncate=False)

    # delta_main_df =

    # Check for inactive campaigns on UI
    # true_df = inactive_campaign_check(true_df, campaigns_df)

    # dynamic_df = main_df.filter(col('campaign_end_date') > current_date()).filter(col('campaign_type') == 'dynamic')
    # dynamic_df.show()
    # static_df = main_df.join(dynamic_df,(main_df.audience == dynamic_df.audience) &
    #                          (dynamic_df.external_id == main_df.external_id),"left_anti")
    # static_df.show()


    merge_true_df: Optional[DataFrame] = None
    # merge_false_df: Optional[DataFrame] = None
    #
    for c in campaign_distinct:
        delta_mapping = delta_df.select('audience', 'external_id', 'campaign_end_date').distinct()
        audience_count = delta_mapping.filter(col('audience') == c['audience']).count()

        if audience_count == 0:


            per_audience_df = main_df.filter(col('audience') == c['audience'])

            true_df = per_audience_df
            print("Audience been checked :", {c['audience']})
            print("No delta records available")
            print("true_df :  ", true_df.count())

            # common = static_df.join(true_df, static_df.external_id == true_df.external_id, 'leftsemi')
            # diff = static_df.subtract(common)
            # diff.show()
            # print(diff.count())
            #
            # static_df = diff
            # false_df: DataFrame = (
            #     main_df.join(true_df, (main_df.business_unit == true_df.business_unit) &
            #                            (main_df.campaign_end_date == true_df.campaign_end_date),
            #                            "left_anti")).withColumn(
            #     "active_appgroup", lit(False))
            # print("false_df :  ", false_df.count())
        else:

            per_audience_df = main_df.filter(col('audience') == c['audience'])

            delta_audience_ids = per_audience_df.join(delta_df,
                                              (main_df.audience == delta_df.audience) & (
                                                      delta_df.external_id == main_df.external_id),
                                              "left_anti")
            print("Audience been checked for delta :", {c['audience']})
            print("Delta Ids Available")
            print("delta_audience_ids :  ", delta_audience_ids.count())
            true_df = delta_audience_ids
            print("true_df After Delta:  ", true_df.count())

        # false_df: DataFrame = (
        #     main_df.join(true_df, (main_df.business_unit == true_df.business_unit) &
        #                            (main_df.campaign_end_date == true_df.campaign_end_date),
        #                            "left_anti")).withColumn(
        #     "active_appgroup", lit(False))
        # print("false_df :  ", false_df.count())
        merge_true_df = true_df if merge_true_df is None else merge_true_df.union(true_df)
    # merge_false_df = false_df if merge_false_df is None else merge_false_df.union(false_df)

    print("merge_true_df", merge_true_df.count())
    false_df: DataFrame = (
        audiencesbraze_df.join(merge_true_df, (audiencesbraze_df.business_unit == merge_true_df.business_unit) &
                               (audiencesbraze_df.campaign_end_date == merge_true_df.campaign_end_date) &
                               (audiencesbraze_df.external_id == merge_true_df.external_id) &
                               (audiencesbraze_df.gcr_id == merge_true_df.gcr_id) &
                               (audiencesbraze_df.share_id == merge_true_df.share_id) &
                               (audiencesbraze_df.audience == merge_true_df.audience),
                               "left_anti")).withColumn("active_appgroup", lit(False))


    print("merge_true_df", merge_true_df.count())
    print("false_df", false_df.count())


    audiences_push_braze = merge_true_df.union(false_df)
    audiences_push_braze = audiences_push_braze.dropDuplicates(['audience', 'external_id', 'business_unit'])

    print("final_total_audiences_to_push", audiences_push_braze.count())

    print("final true_df", merge_true_df.count())
    print("final false_df", false_df.count())

    show_audience = audiences_push_braze.filter(col("active_appgroup") == True).select('audience').distinct()
    show_audience.show(truncate=False)

    audiences_push_braze.show(1, truncate=False)

    return audiences_push_braze


def monitoring_audience_for_notification(spark_session: SparkSession, audiences: DataFrame,
                                         processed_audience: DataFrame):
    audiences.show(1)
    processed_audience.show(truncate=False)

    processed_audience1: DataFrame = (
        audiences.join(processed_audience, audiences.audience == processed_audience.audience, "inner")) \
        .drop(processed_audience.audience)
    print(processed_audience1.count())
    processed_audience1.show()

    processed_campaign: DataFrame = (
        processed_audience1.dropDuplicates(['audience']).withColumn("campaign_name", col('audience'))
        .withColumn('start_timestamp', current_timestamp())
        .withColumn('end_timestamp', current_timestamp())
        .withColumn('processed_record', lit(processed_audience1.processed_records))
        .withColumn('failed_record', lit(processed_audience1.exsell_records - processed_audience1.processed_records))
        # .withColumn('start_date', lit(processed_audience1.campaign_start_date))
        .withColumn('end_date', lit(processed_audience1.campaign_end_date))
        .withColumn('status', lit('SUCCESS'))
        .withColumn('exsell_record', lit(processed_audience1.exsell_records))
    ).select("campaign_name", "start_timestamp", "end_timestamp", "processed_record", "failed_record", "end_date",
             "status", "exsell_record")
    print("processed_campaign")
    print(f"Processed Status is found for below list of campaigns")
    processed_campaign.show(truncate=False)

    return processed_campaign


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
    # customer_profiles = spark.read.parquet('s3://cvm-uat-conformed-d5b175d/cusotmer_profiles_delta/customer_profile/')

    id_mapping = DeltaTable.forPath(spark, get_s3_path(conformed.IdMapping, lake_descriptor)).toDF()


    # braze_attributes = spark.read.parquet('s3://cvm-prod-landing-5d6c06b/test/subset_2023-07-26/')
    # braze_attributes = spark.read.parquet('s3://cvm-prod-landing-5d6c06b/bu_external_id/2023-07-04/')
    braze_attributes = create_delta_table_from_catalog(spark, conformed.BuExternalId, lake_descriptor).toDF()

    braze_attributes = spark.read.parquet('s3://cvm-prod-landing-5d6c06b/test/subset_2023-07-26/')
    # braze_attributes = spark.read.parquet('s3://cvm-prod-landing-5d6c06b/bu_external_id/2023-07-04/')
    # braze_attributes = create_delta_table_from_catalog(spark, conformed.BuExternalId, lake_descriptor).toDF()

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
    promos_df = promos_df.withColumn("qty_min", lit(" ")).withColumn("qty_max", lit(" ")).withColumn(
        "ind_can_have_other_discount", lit(" ")).withColumn("ind_apply_after_tax", lit(" ")).withColumn(
        "ind_can_apply_to_tab", lit(" ")).withColumn("ind_applies_when_purchasing_products", lit(" ")).withColumn(
        "ind_applies_to_products", lit(" "))
    id_map_df = create_delta_table_from_catalog(spark, conformed.IdMapping, lake_descriptor).toDF()
    audiences_df = create_delta_table_from_catalog(spark, conformed.Audiences, lake_descriptor).toDF()
    creat_delta_table_if_not_exists(spark, conformed.AudiencesBraze, lake_descriptor)
    audiencesbraze_path = get_s3_path(conformed.AudiencesBraze, lake_descriptor)
    audiencesbrazepush = DeltaTable.forPath(spark, audiencesbraze_path)

    creat_delta_table_if_not_exists(spark, conformed.DeltaMapping, lake_descriptor)
    audiencesdelta_path = get_s3_path(conformed.DeltaMapping, lake_descriptor)
    audiencesdelta = DeltaTable.forPath(spark, audiencesdelta_path)
    delta_df = create_delta_table_from_catalog(spark, conformed.DeltaMapping, lake_descriptor).toDF()

    audience_info: DataFrame = glueContext.create_dynamic_frame_from_options(
        connection_type='mysql',
        connection_options={
            "useConnectionProperties": "true",
            "dbtable": 'cdp_audience_information',
            "connectionName": rds_connection,
        }
    ).toDF()

    campaigns_df: DataFrame = glueContext.create_dynamic_frame_from_options(
        connection_type='mysql',
        connection_options={
            "useConnectionProperties": "true",
            "dbtable": 'offerbank_campaigns',
            "connectionName": rds_connection,
        }
    ).toDF()

    offers_bank_offers: DataFrame = glueContext.create_dynamic_frame_from_options(
        connection_type='mysql',
        connection_options={
            "useConnectionProperties": "true",
            "dbtable": 'offerbank_gravityoffer',
            "connectionName": rds_connection,
        }
    ).toDF()

    result = braze_audience_builder(
        spark, audience_info, customer_profiles, braze_attributes, campaigns_df, id_map_df, promos_df,
        offers_bank_offers, delta_df
    )
    print("printing run output for braze_audience")
    result.braze_audience.show(truncate=False)
    print(result.braze_audience.count())
    monitoring_audience_for_notification = monitoring_audience_for_notification(spark, result.braze_audience,
                                                                                result.processed_audience)

    # Audiences for current run write to athena table
    write_delta(
        result.braze_audience, conformed.Audiences, spark, lake_descriptor, save_mode="overwrite"
    )
    # Merging data for audiences table for array push to braze
    audiencesbrazepush.alias(conformed.AudiencesBraze.table_alias()).merge(
        result.braze_audience.alias('braze_audience'),
        f"""
            braze_audience.external_id = {conformed.AudiencesBraze.external_id.column_alias()} and
            braze_audience.gcr_id = {conformed.AudiencesBraze.gcr_id.column_alias()} and
            braze_audience.business_unit = {conformed.AudiencesBraze.business_unit.column_alias()} and
            braze_audience.audience = {conformed.AudiencesBraze.audience.column_alias()} and 
            braze_audience.campaign_end_date = {conformed.AudiencesBraze.campaign_end_date.column_alias()}
            """
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    audiencesbraze_df = create_delta_table_from_catalog(spark, conformed.AudiencesBraze, lake_descriptor).toDF()
    print("Count of records to be merged ", result.braze_audience.count())
    print("Count for total audiencesbraze_df after ",audiencesbraze_df.count())
    audience_push_to_braze = audience_push_to_braze(spark, result.braze_audience, audiencesbraze_df, delta_df , campaigns_df)
    print("Count for true false ready for merge after ", audience_push_to_braze.count())
    audiencesbrazepush.alias(conformed.AudiencesBraze.table_alias()).merge(
        audience_push_to_braze.alias('braze_audience'),
        f"""
            braze_audience.external_id = {conformed.AudiencesBraze.external_id.column_alias()} and
            braze_audience.gcr_id = {conformed.AudiencesBraze.gcr_id.column_alias()} and
            braze_audience.business_unit = {conformed.AudiencesBraze.business_unit.column_alias()} and
            braze_audience.audience = {conformed.AudiencesBraze.audience.column_alias()} and 
            braze_audience.campaign_end_date = {conformed.AudiencesBraze.campaign_end_date.column_alias()}
            """
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    audience_display = result.delta_audience.select('audience').distinct()

    print("Delta mapping table entry for campaigns", audience_display.show(truncate=False))

    audiencesdelta.alias(conformed.DeltaMapping.table_alias()).merge(
        result.delta_audience.alias('braze_audience'),
        f"""
                 braze_audience.external_id = {conformed.DeltaMapping.external_id.column_alias()} and
                 braze_audience.business_unit = {conformed.DeltaMapping.business_unit.column_alias()} and
                 braze_audience.audience = {conformed.DeltaMapping.audience.column_alias()} and
                 braze_audience.gcr_id = {conformed.DeltaMapping.gcr_id.column_alias()}
                 """
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()


    print("Delta mapping table entry done for campaigns: ", audience_display.show(truncate=False))

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

    processed_campaign_to_update = monitoring_audience_for_notification.alias('latest').join(
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

