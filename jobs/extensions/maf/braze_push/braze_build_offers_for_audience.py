import sys
from typing import Optional, NamedTuple

import sqlglot
from pyspark import SparkContext
from pyspark.sql import DataFrame, Column, SparkSession
from delta.tables import *
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
from cvmdatalake.transformations.promotions import get_eligible_promotions

total_offers_assigned_to_customer = 10


def get_promos_ids_from_offers_bank(aud_info: Row, offer_bank_offers: DataFrame) -> list[str]:
    print(aud_info)
    offer_bank_offer_ids: str = aud_info[landing.Campaign.offer_concat.name]
    offer_bank_offer_ids: list[str] = str.split(offer_bank_offer_ids, '+')

    offer_names = (
        offer_bank_offers
        .filter(col(landing.Offer.id.name).cast(StringType()).isin(offer_bank_offer_ids))
        .select(landing.Offer.offer_name.name)
    )

    promo_ids = offer_names.select(regexp_extract(col(landing.Offer.offer_name.name), "\((\d+)\)", 1)).collect()
    return list(map(lambda x: x[0], promo_ids))


class RunResult(NamedTuple):
    static_nbo_for_braze: DataFrame
    static_nbo_for_share: DataFrame
    static_offer_to_upsert: DataFrame


def overwrite_nbo_with_static_offers_for_audience(
        audience_with_ids: DataFrame, promos: DataFrame, offer_bank_offers: DataFrame, campaigns: DataFrame
) -> tuple[DataFrame, DataFrame]:
    audience_with_ids.show(5, truncate=False)
    audience_with_ids.collect()

    campaigns = campaigns.withColumn(
        landing.Campaign.audience_id.name,
        explode(split(landing.Campaign.audiences_concat.name, "\+"))
    )

    audience_with_ids = audience_with_ids.join(
        how="inner",
        other=campaigns.alias(landing.Campaign.table_alias()),
        on=col("audience") == col(landing.Campaign.name.column_alias())
    )

    audience_with_ids.count()
    audience = audience_with_ids.select('audience', 'offer_concat').distinct().collect()
    nbo_to_overwrite: Optional[DataFrame] = None
    static_offers_to_upsert_to_dynamodb: Optional[DataFrame] = None

    for ai in audience:
        # Assign default offers based on eligibility criteria
        eligible_promos = get_eligible_promotions(promos)
        default_ids = list(map(lambda x: x[0], eligible_promos.select(Promotions.idi_offer.name).collect()))
        print(ai)
        # Assign static offers based on audience
        offer_bank_offer_ids = get_promos_ids_from_offers_bank(ai, offer_bank_offers)
        print(f"Offers assigned to audience in UI {ai[landing.Campaign.name.name]}: {offer_bank_offer_ids}")

        number_of_default_offers = max(0, total_offers_assigned_to_customer - len(offer_bank_offer_ids))

        offer_assignment = (
            audience_with_ids.filter({ai[landing.Campaign.name.name]})
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
        print(f"All offers assigned to audience {ai[landing.Campaign.name.name]}: {offer_ids}")

        offer_columns = [c.name for c in conformed.Promotions]
        static_offers = (
            promos
            .filter(col(conformed.Promotions.idi_offer.name).isin(offer_ids))
            .withColumn('details', to_json(struct(*offer_columns)))
            .select(Promotions.idi_offer.name, 'details')
        )
        print(
            f"Offers for audience {ai[landing.Campaign.name.name]} after mapping to promotions: {get_show_str(static_offers, truncate=False)}"
        )

        nbo_to_overwrite = (
            nbo_to_overwrite.union(offer_assignment)
            if nbo_to_overwrite is not None else offer_assignment
        )

        static_offers_to_upsert_to_dynamodb = (
            static_offers_to_upsert_to_dynamodb.union(static_offers)
            if static_offers_to_upsert_to_dynamodb is not None else static_offers
        )

    if nbo_to_overwrite is None:
        raise Exception("There are no new audiences to push")

    braze_to_overwrite, share_to_overwrite = final_prep_for_nbo_to_overwrite(nbo_to_overwrite.select(
        nbo_to_overwrite.gcr_id, nbo_to_overwrite.braze_id, nbo_to_overwrite.share_id, nbo_to_overwrite.idi_offer
    ))

    discrepancies = find_discrepancies(audience_with_ids, 'external_id', braze_to_overwrite, 'braze_id')
    print(
        f"{discrepancies.count()} discrepancies for braze with nbo_to_overwrite  {get_show_str(discrepancies, truncate=False)}"
    )

    return RunResult(
        static_nbo_for_braze=braze_to_overwrite,
        static_nbo_for_share=share_to_overwrite,
        static_offer_to_upsert=static_offers_to_upsert_to_dynamodb.dropDuplicates([Promotions.idi_offer.name])
    )


#
#
def get_n_offers_at_random(offer_ids: list[str], n: int) -> Column:
    return slice(
        shuffle(array(*map(lit, offer_ids))),
        start=1, length=n
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
        nbo_to_overwrite.withColumnRenamed("external_id", 'braze_id')
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

    promos_df = create_delta_table_from_catalog(spark, conformed.Promotions, lake_descriptor).toDF()
    audiences_df = create_delta_table_from_catalog(spark, conformed.Audiences, lake_descriptor).toDF()
    # audiences = create_delta_table_from_catalog(spark, conformed.Audiences, lake_descriptor)

    offers_bank_offers: DataFrame = glueContext.create_dynamic_frame_from_options(
        connection_type='mysql',
        connection_options={
            "useConnectionProperties": "true",
            "dbtable": 'offerbank_offers',
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

    result = overwrite_nbo_with_static_offers_for_audience(audiences_df, promos_df, offers_bank_offers, campaigns_df)

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