from pyspark.sql.functions import *
from pyspark.sql.functions import when, col, unix_timestamp

from cvmdatalake.conformed import Promotions

_promotions_mandatory_columns = [
    Promotions.idi_offer,
    Promotions.nam_offer,
    Promotions.des_subtitle,
    Promotions.ind_member_visible,
    Promotions.des_offer,
    Promotions.des_url_image,
    Promotions.des_url_mobile_image,
    Promotions.dat_period_start,
    # Promotions.dat_period_end,
    Promotions.des_offer_section,
    Promotions.des_offer_type,
    Promotions.des_terms,
    Promotions.cde_sponsor_key,
    Promotions.nam_sponsor,
    Promotions.des_url_logo,
    Promotions.ind_is_template_based,
    # Promotions.des_brand,
    Promotions.des_bu
]


def get_eligible_promotions(promotions: DataFrame) -> DataFrame:
    eligible_promos = filter_promotions_for_eligibility(promotions)
    targetted_offers = filter_segment_for_eligibility(promotions)
    return map_promotions_to_details(eligible_promos), map_promotions_to_details(targetted_offers)


def filter_promotions_for_eligibility(promo: DataFrame) -> DataFrame:
    print(f"Promotions before filtering for eligibility: {promo.count()}")

    eligible_promos = promo.filter(
        (lower(Promotions.des_restrictions.name) == 'all') &
        ((lower(Promotions.des_status.name) == 'launched') | (lower(Promotions.des_status.name) == 'update')) &
        (col(Promotions.dat_period_end.name).isNull() | (col(Promotions.dat_period_end.name) > current_date()))
    )

    for c in _promotions_mandatory_columns:
        eligible_promos = eligible_promos.filter(col(c.name).isNotNull())

    # TODO: replace with lazy logging, so spark won't try to count when correct logging level is not set
    print(f"Promotions after filtering for eligibility: {eligible_promos.count()}")
    return eligible_promos


def filter_segment_for_eligibility(promo: DataFrame) -> DataFrame:
    print(f"Promotions before filtering targetted offers: {promo.count()}")

    targetted_offers = promo.filter(
        (lower(Promotions.des_restrictions.name) == 'segment') &
        ((lower(Promotions.des_status.name) == 'launched') | (lower(Promotions.des_status.name) == 'update')) &
        (col(Promotions.dat_period_end.name).isNull() | (col(Promotions.dat_period_end.name) > current_date()))
    )

    for c in _promotions_mandatory_columns:
        targetted_offers = targetted_offers.filter(col(c.name).isNotNull())

    # TODO: replace with lazy logging, so spark won't try to count when correct logging level is not set
    print(f"Promotions after filtering targetted offers: {targetted_offers.count()}")
    return targetted_offers


def map_promotions_to_details(promotions: DataFrame, additional_column: str = None) -> DataFrame:
    offer_columns = [c.name for c in Promotions if c.name in promotions.columns]
    eligible_promos = promotions.withColumn('details', to_json(struct(*offer_columns)))

    returned_columns = [Promotions.idi_offer.name, 'details']

    if additional_column is not None:
        returned_columns.append(additional_column)

    return eligible_promos.select(returned_columns)


def enrich_with_expiry_column(promotions: DataFrame) -> DataFrame:
    return promotions.withColumn(
        'expires_at',
        when(
            col(Promotions.dat_period_end.name).isNotNull(),
            unix_timestamp(col(Promotions.dat_period_end.name))
        )
    )
