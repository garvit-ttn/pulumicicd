import json

from delta.tables import DeltaTable
from pyspark import Row
from pyspark.sql.functions import lit, regexp_extract, col

import jobs.extensions.maf.braze_push.audience_builder as audience_builder
from cvmdatalake import landing, conformed
from jobs.extensions.maf.braze_push.audiences import map_app_group, batched


def test_braze_audience_building(spark):
    audience_info = spark.read.parquet('/home/sad/Downloads/audience_info')
    campaigns = spark.read.parquet('/home/sad/Downloads/campaign')
    offers = spark.read.parquet('/home/sad/Downloads/offerrbank_offers.parquet')
    promotions = spark.read.parquet('/home/sad/Downloads/promotions')
    id_map = DeltaTable.forPath(spark, '/home/sad/Downloads/id_mapping').toDF()

    profiles = spark.read.parquet('/home/sad/Downloads/audience_info')

    result = audience_builder.run(
        spark, audience_info, profiles, campaigns,
        promotions, offers, id_map
    )

    print(result.braze_audience.collect())
    print(result.static_nbo.collect())


def test_get_promos_ids_from_offers_bank(spark):
    aud_info = Row(**{landing.Campaign.offer_concat.name: "61059+61060+61061"})
    offers = spark.read.parquet("/home/sad/Downloads/offerrbank_offers.parquet")

    result = audience_builder.get_promos_ids_from_offers_bank(
        aud_info=aud_info, offer_bank_offers=offers
    )

    assert len(result) == 3
    assert "115" in result
    assert "2055610099" in result
    assert "1508629853" in result


def test_get_promos_ids_from_offers_bank_single(spark):
    aud_info = Row(**{landing.Campaign.offer_concat.name: "61059"})
    offers = spark.read.parquet("/home/sad/Downloads/offerrbank_offers.parquet")

    result = audience_builder.get_promos_ids_from_offers_bank(
        aud_info=aud_info, offer_bank_offers=offers
    )

    assert len(result) == 1
    assert "115" in result


def test_overwrite_nbo_with_static_offers_for_audience(spark):
    audiences = spark.createDataFrame([
        {conformed.CustomerProfile.idi_counterparty.name: '17913875'}
    ])

    offers = spark.read.parquet("/home/sad/Downloads/offerrbank_offers.parquet")
    id_map = DeltaTable.forPath(spark, '/home/sad/Downloads/id_mapping').toDF()
    promotions = spark.read.parquet('/home/sad/Downloads/promotions').withColumn(
        conformed.Promotions.bu.name, lit('share')
    )
    aud_info = Row(**{
        landing.Campaign.offer_concat.name: "61059+61060+61061",
        landing.Campaign.name.name: "test_campaign"
    })

    audience_with_offers = audience_builder.overwrite_nbo_with_static_offers_for_audience(
        audience=audiences, aud_info=aud_info, offer_bank_offers=offers, promos=promotions
    )

    print(audience_with_offers.show(truncate=False))


def test_problematic_offer(spark):
    offer_names = spark.createDataFrame([
        {'offer_name': 'Unlock 10X on Your Groceries (2035139325)'}
    ])

    promo_ids = offer_names.select(regexp_extract(col(landing.Offer.offer_name.name), "\((\d+)\)", 1)).collect()
    result = list(map(lambda x: x[0], promo_ids))

    assert len(result) == 1
    assert '2035139325' in result


def test_final_prep_for_nbo_to_overwrite(spark):
    nbo_to_overwrite = spark.createDataFrame([
        {
            'gcr_id': '17913875',
            'braze_id': 'vmYQRMawqPZ6/mOXZ1HkCQ==',
            'share_id': '9478400018709960',
            'offers': [json.dumps({'name': 'offer1'}), json.dumps({'name': 'offer2'})]
        },
        {
            'gcr_id': '17913875',
            'braze_id': 'vmYQRMawqPZ6/mOXZ1HkCQ==',
            'share_id': '9478400018709960',
            'offers': [json.dumps({'name': 'offer3'}), json.dumps({'name': 'offer4'}), json.dumps({'name': 'offer2'})]
        },
    ])

    nbo = audience_builder.final_prep_for_nbo_to_overwrite(nbo_to_overwrite)
    print(nbo.show(truncate=False))


def test_app_group_mapping_returns_xbu_for_multiple_groups():
    braze_app_groups = map_app_group("CRF+LEC+SMBU")

    assert len(braze_app_groups) == 1
    assert "XBU" in braze_app_groups


def test_app_group_mapping_returns_app_group():
    braze_app_groups = map_app_group("CRF")

    assert len(braze_app_groups) == 1
    assert "CRF" in braze_app_groups


def test_app_group_mapping_translates_share_to_lyl():
    braze_app_groups = map_app_group("SHARE")

    assert len(braze_app_groups) == 1
    assert "LYL" in braze_app_groups


def test_app_group_mapping_returns_empty_when_blank():
    braze_app_groups = map_app_group("")

    assert len(braze_app_groups) == 0


def test_sequence_batching():
    result = batched(list(range(0, 11)), 3)

    for b in result[:-1]:
        assert len(b) == 3

    assert len(result[-1:][0]) == 2
