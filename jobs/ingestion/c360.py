from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import LongType, FloatType, IntegerType, DateType

from pyspark.sql import functions as F
from pyspark.sql.functions import col
import pyspark.sql.functions as func

from cvmdatalake import creat_delta_table_if_not_exists, get_s3_path, features, convert_columns_to_lower_case
from cvmdatalake.conformed import *

demographics_columns_for_c360 = [
    Demographics.bu.name,
    Demographics.cod_sor_counterparty.name,
    Demographics.cde_country_residence.name,
    Demographics.idi_counterparty.name,
    Demographics.ind_person_gender.name,
    Demographics.adr_visit_address_string.name,
    Demographics.adr_visit_local_specific.name,
    Demographics.adr_visit_cityname.name,
    Demographics.des_age.name,
    Demographics.cdi_counterparty_role.name,
    Demographics.dat_of_birth.name
    # missing nationality -> filled up with empty value later in the script
]

nbo_output_columns_for_360 = [
    # CustomerProfile.idi_counterparty.name,
    CustomerProfile.nbo1.name,
    CustomerProfile.nbo2.name,
    CustomerProfile.nbo3.name,
    CustomerProfile.nbo4.name,
    CustomerProfile.nbo5.name
]

rfm_columns = [
    CustomerProfile.recency_proposition_level_3.name,
    CustomerProfile.frequency_proposition_level_3.name,
    CustomerProfile.monetary_proposition_level_3.name,
    CustomerProfile.r_rank_norm_proposition_level_3.name,
    CustomerProfile.f_rank_norm_proposition_level_3.name,
    CustomerProfile.m_rank_norm_proposition_level_3.name,
    CustomerProfile.customer_segment_proposition_level_3.name
]

clv_columns_for_c360 = [
    CustomerProfile.max_date.name,
    CustomerProfile.frequency.name,
    CustomerProfile.monetary.name,
    CustomerProfile.customer_age.name,
    CustomerProfile.expected_purchases_3m.name,
    CustomerProfile.clv_3m.name,
    CustomerProfile.p_alive.name,
]

braze_columns_for_c360 = [
    CustomerProfile.dat_enrollment.name,
    CustomerProfile.nam_enrollment_sponsor.name,
    CustomerProfile.dat_birth.name,
    CustomerProfile.dat_registration_share.name,
    CustomerProfile.nam_title.name,
    CustomerProfile.qty_points_balance.name,
    CustomerProfile.dat_last_accrual.name,
    CustomerProfile.dat_last_activity.name,
    CustomerProfile.dat_last_redemption.name,
    CustomerProfile.qty_accrued_total.name,
    CustomerProfile.qty_redeemed_total.name,
    CustomerProfile.cde_country_of_residence.name,
    CustomerProfile.des_membership_stage.name,
    CustomerProfile.dat_last_login.name,
    CustomerProfile.des_device_model.name,
    CustomerProfile.des_device_os.name,
    CustomerProfile.des_device_carrier.name,
    CustomerProfile.qty_sessions.name,
    CustomerProfile.dat_first_session.name,
    CustomerProfile.dat_last_session.name,
    CustomerProfile.nam_country.name,
    CustomerProfile.sw_completion_status.name,
    CustomerProfile.sw_grocery_last_bit_date.name,
    CustomerProfile.sw_grocery_status.name,
    CustomerProfile.sw_hotel_last_bit_date.name,
    CustomerProfile.sw_hotel_status.name,
    CustomerProfile.sw_l_and_e_last_bit_date.name,
    CustomerProfile.sw_l_and_e_status.name,
    CustomerProfile.sw_lifestyle_last_bit_date.name,
    CustomerProfile.sw_lifestyle_status.name,
    CustomerProfile.sw_mall_last_bit_date.name,
    CustomerProfile.sw_mall_status.name,
    CustomerProfile.sw_segment_code.name,
    CustomerProfile.fab_cobrand_product.name,
    CustomerProfile.favfruit.name,
    CustomerProfile.share_active_30_days.name,
    CustomerProfile.share_active_60_days.name,
    CustomerProfile.share_active_90_days.name,
    CustomerProfile.share_active_180_days.name,
    CustomerProfile.ind_coupon_generated.name,
    CustomerProfile.dat_first_login.name,
    CustomerProfile.dat_first_login_share.name,
    CustomerProfile.gulf_news.name,
    CustomerProfile.isd_code.name,
    CustomerProfile.cde_mal.name,
    CustomerProfile.offline_first_maf_contact.name,
    CustomerProfile.qty_points_won_spin_wheel.name,
    CustomerProfile.des_language_preferred.name,
    CustomerProfile.registered_from.name,
    CustomerProfile.dat_registration.name,
    CustomerProfile.des_share_world_segment.name,
    CustomerProfile.des_summer_activity.name,
    CustomerProfile.ide_cc_mall_code.name,
    CustomerProfile.des_interests.name,
    CustomerProfile.ind_logged_in.name,
    CustomerProfile.qty_payment_card.name,
    CustomerProfile.lifestyle_total_txn_last_last_6_months.name,
    CustomerProfile.lifestyle_avg_spend_last_6_months.name,
    CustomerProfile.lifestyle_avg_spend_last_3_months.name,
    CustomerProfile.lifestyle_avg_spend_last_2_months.name,
    CustomerProfile.lifestyle_avg_spend_last_1_month.name,
    CustomerProfile.lifestyle_luxury_customer_flag.name,
    CustomerProfile.nam_brand.name,
    CustomerProfile.lifestyle_furniture_category_spend_last_3_months.name,
    CustomerProfile.lifestyle_furniture_category_spend_last_1_month.name,
    CustomerProfile.lifestyle_furniture_category_spend_last_7_days.name,
    CustomerProfile.lifestyle_decor_category_spend_last_3_months.name,
    CustomerProfile.lifestyle_decor_category_spend_last_1_month.name,
    CustomerProfile.lifestyle_decor_category_spend_last_7_days.name,
    CustomerProfile.lifestyle_kids_category_spend_last_3_months.name,
    CustomerProfile.lifestyle_kids_category_spend_last_1_month.name,
    CustomerProfile.lifestyle_kids_category_spend_last_7_days.name,
    CustomerProfile.des_language_customer.name,
    CustomerProfile.vox_avg_txn_value_for_12_months.name,
    CustomerProfile.vox_total_spend_for_12_months.name,
    CustomerProfile.vox_arabic_movie_count.name,
    CustomerProfile.vox_hindi_movie_count.name,
    CustomerProfile.vox_children_movie_count.name,
    CustomerProfile.vox_cinema_total_spend_last_12_months.name,
    CustomerProfile.vox_cinema_total_spend_last_6_months.name,
    CustomerProfile.vox_cinema_total_spend_last_3_months.name,
    CustomerProfile.maf_cinema_total_txn_last_12_months.name,
    CustomerProfile.maf_cinema_total_txn_last_6_months.name,
    CustomerProfile.maf_cinema_total_txn_last_3_months.name,
    CustomerProfile.vox_total_number_transactions.name,
    CustomerProfile.vox_family_movie_total_revenue.name,
    CustomerProfile.vox_family_movie_total_txn.name,
    CustomerProfile.vox_scifi_movie_total_txn.name,
    CustomerProfile.vox_arabic_movie_total_txn.name,
    CustomerProfile.vox_kids_movie_total_txn.name,
    CustomerProfile.vox_preferred_mall.name,
    CustomerProfile.vox_luxury_concept_flag.name,
    CustomerProfile.vox_most_recent_movie_watched.name,
    CustomerProfile.vox_number_movies_last_1_month.name,
    CustomerProfile.lne_avg_txn_for_12_months_246.name,
    CustomerProfile.lne_avg_txn_for_12_months_267.name,
    CustomerProfile.lne_no_txn_for_12_months.name,
    CustomerProfile.lne_total_spend_for_12_months.name,
    CustomerProfile.lne_total_transactions_last_6_months.name,
    CustomerProfile.lne_total_transactions_last_3_months.name,
    CustomerProfile.lne_customer_luxury_flag.name,
    CustomerProfile.lec_total_spend_for_12_months.name,
    CustomerProfile.lec_mp_total_txn_last_6_months.name,
    CustomerProfile.lec_mp_total_txn_last_3_months.name,
    CustomerProfile.lec_mp_avg_txn_value_for_12_months.name,
    CustomerProfile.ski_total_txn_last_6_months.name,
    CustomerProfile.ski_total_txn_last_3_months.name,
    CustomerProfile.ski_avg_txn_value_last_12_months.name,
    CustomerProfile.app_id.name
]

brand_level_for_c360 = [
    CustomerProfile.lifecycle_stage_als.name,
    CustomerProfile.lifecycle_stage_carrefour.name,
    CustomerProfile.lifecycle_stage_ccd.name,
    CustomerProfile.lifecycle_stage_ccmi.name,
    CustomerProfile.lifecycle_stage_cnb.name,
    CustomerProfile.lifecycle_stage_crf.name,
    CustomerProfile.lifecycle_stage_lec.name,
    CustomerProfile.lifecycle_stage_lifestyle.name,
    CustomerProfile.lifecycle_stage_lll.name,
    CustomerProfile.lifecycle_stage_magicp.name,
    CustomerProfile.lifecycle_stage_moe.name,
    CustomerProfile.lifecycle_stage_share.name,
    CustomerProfile.lifecycle_stage_skidub.name,
    CustomerProfile.lifecycle_stage_smbu.name,
    CustomerProfile.lifecycle_stage_vox.name,
    CustomerProfile.purchase_propensity_als_next_3_months.name,
    CustomerProfile.purchase_propensity_carrefour_next_3_months.name,
    CustomerProfile.purchase_propensity_ccd_next_3_months.name,
    CustomerProfile.purchase_propensity_ccmi_next_3_months.name,
    CustomerProfile.purchase_propensity_cnb_next_3_months.name,
    CustomerProfile.purchase_propensity_crfnext_3_months.name,
    CustomerProfile.purchase_propensity_lec_next_3_months.name,
    CustomerProfile.purchase_propensity_lifestyle_next_3_months.name,
    CustomerProfile.purchase_propensity_lll_next_3_months.name,
    CustomerProfile.purchase_propensity_magicp_next_3_months.name,
    CustomerProfile.purchase_propensity_moe_next_3_months.name,
    CustomerProfile.purchase_propensity_share_next_3_months.name,
    CustomerProfile.purchase_propensity_skidub_next_3_months.name,
    CustomerProfile.purchase_propensity_smbu_next_3_months.name,
    CustomerProfile.purchase_propensity_vox_next_3_months.name,
    CustomerProfile.total_spend_als_last_3_months.name,
    CustomerProfile.total_spend_als_last_6_months.name,
    CustomerProfile.total_spend_carrefour_last_3_months.name,
    CustomerProfile.total_spend_carrefour_last_6_months.name,
    CustomerProfile.total_spend_ccd_last_3_months.name,
    CustomerProfile.total_spend_ccd_last_6_months.name,
    CustomerProfile.total_spend_ccmi_last_3_months.name,
    CustomerProfile.total_spend_ccmi_last_6_months.name,
    CustomerProfile.total_spend_cnb_last_3_months.name,
    CustomerProfile.total_spend_cnb_last_6_months.name,
    CustomerProfile.total_spend_crf_last_3_months.name,
    CustomerProfile.total_spend_crf_last_6_months.name,
    CustomerProfile.total_spend_lec_last_3_months.name,
    CustomerProfile.total_spend_lec_last_6_months.name,
    CustomerProfile.total_spend_lifestyle_last_3_months.name,
    CustomerProfile.total_spend_lifestyle_last_6_months.name,
    CustomerProfile.total_spend_lll_last_3_months.name,
    CustomerProfile.total_spend_lll_last_6_months.name,
    CustomerProfile.total_spend_magicp_last_3_months.name,
    CustomerProfile.total_spend_moe_last_12_months.name,
    CustomerProfile.total_spend_moe_last_1_day.name,
    CustomerProfile.total_spend_moe_last_1_month.name,
    CustomerProfile.total_spend_moe_last_3_months.name,
    CustomerProfile.total_spend_moe_last_6_months.name,
    CustomerProfile.total_spend_moe_last_7_days.name,
    CustomerProfile.total_spend_share_last_3_months.name,
    CustomerProfile.total_spend_share_last_6_months.name,
    CustomerProfile.total_spend_share_last_7_days.name,
    CustomerProfile.total_spend_skidub_last_3_months.name,
    CustomerProfile.total_spend_skidub_last_6_months.name,
    CustomerProfile.total_spend_smbu_last_3_months.name,
    CustomerProfile.total_spend_smbu_last_6_months.name,
    CustomerProfile.total_spend_vox_last_3_months.name,
    CustomerProfile.total_spend_vox_last_6_months.name,
]

new_columns_fro_360 = [
        CustomerProfile.total_transactions_share_partner_last_1_day.name,
        CustomerProfile.total_transactions_share_partner_last_7_days.name,
        CustomerProfile.total_transactions_share_partner_last_1_month.name,
        CustomerProfile.total_transactions_share_partner_last_3_months.name,
        CustomerProfile.total_transactions_share_partner_last_6_months.name,
        CustomerProfile.total_transactions_share_partner_last_12_months.name,
        CustomerProfile.total_spend_share_partner_last_1_day.name,
        CustomerProfile.total_spend_share_partner_last_7_days.name,
        CustomerProfile.total_spend_share_partner_last_1_month.name,
        CustomerProfile.total_spend_share_partner_last_3_months.name,
        CustomerProfile.total_spend_share_partner_last_6_months.name,
        CustomerProfile.total_spend_share_partner_last_12_months.name,
        CustomerProfile.partner_first_transaction_date.name,
        CustomerProfile.partner_last_transaction_date.name,
        CustomerProfile.partner_engagement_status.name,
        CustomerProfile.purchase_propensity_luxury.name,
        CustomerProfile.purchase_propensity_moe_luxury_next_3_months.name,
        CustomerProfile.purchase_propensity_moe_groceries_next_3_months.name,
        CustomerProfile.purchase_propensity_moe_fnb_next_3_months.name,
        CustomerProfile.purchase_propensity_moe_fashion_accessories_next_3_months.name,
        CustomerProfile.purchase_propensity_ccmi_luxury_next_3_months.name,
        CustomerProfile.purchase_propensity_ccmi_fashion_accessories_next_3_months.name,
        CustomerProfile.purchase_propensity_ccmi_homefurniture_electronics_next_3_months.name,
        CustomerProfile.purchase_propensity_ccmi_beauty_next_3_months.name,
        CustomerProfile.purchase_propensity_ccmi_fnb_next_3_months.name,
        CustomerProfile.nbo6.name,
        CustomerProfile.total_spend_moe_luxury_last_7_days.name,
        CustomerProfile.total_spend_moe_luxury_last_1_month.name,
        CustomerProfile.total_spend_moe_luxury_last_3_months.name,
        CustomerProfile.total_spend_moe_luxury_last_6_months.name,
        CustomerProfile.total_spend_moe_luxury_last_12_months.name,
        CustomerProfile.total_spend_ccmi_luxury_last_7_days.name,
        CustomerProfile.total_spend_ccmi_luxury_last_1_month.name,
        CustomerProfile.total_spend_ccmi_luxury_last_3_months.name,
        CustomerProfile.total_spend_ccmi_luxury_last_6_months.name,
        CustomerProfile.total_spend_ccmi_luxury_last_12_months.name,
        CustomerProfile.next_best_share_partners.name,
        CustomerProfile.next_best_category_moe.name,
        CustomerProfile.next_best_category_ccmi.name,
        CustomerProfile.next_best_category_share.name,
        CustomerProfile.next_best_category_lifestyle.name,
        CustomerProfile.next_best_category_that.name,
        CustomerProfile.next_best_category_cnb.name,
        CustomerProfile.next_best_category_lll.name,
        CustomerProfile.next_best_category_lego.name,
        CustomerProfile.next_best_category_als.name,
        CustomerProfile.next_best_category_cb2.name,
        CustomerProfile.next_best_category_anf.name,
        CustomerProfile.next_best_category_hco.name,
        CustomerProfile.next_best_category_shi.name,
        CustomerProfile.next_best_category_pf.name,
        CustomerProfile.next_best_product_lifestyle_by_brand.name,
        CustomerProfile.experiences.name,
        CustomerProfile.category_group.name,
        CustomerProfile.store_category.name,
        CustomerProfile.total_transactions_share_partner_fab_last_1_day.name,
        CustomerProfile.total_transactions_share_partner_fab_last_7_day.name,
        CustomerProfile.total_transactions_share_partner_fab_last_1_months.name,
        CustomerProfile.total_transactions_share_partner_fab_last_3_months.name,
        CustomerProfile.total_transactions_share_partner_fab_last_6_months.name,
        CustomerProfile.total_transactions_share_partner_fab_last_12_months.name,
        CustomerProfile.total_spend_share_fab_last_1_day.name,
        CustomerProfile.total_spend_share_fab_last_7_day.name,
        CustomerProfile.total_spend_share_fab_last_1_month.name,
        CustomerProfile.total_spend_share_fab_last_3_month.name,
        CustomerProfile.total_spend_share_fab_last_6_month.name,
        CustomerProfile.total_spend_share_fab_last_12_month.name,
        CustomerProfile.share_partner_fab_first_transaction_date.name,
        CustomerProfile.share_partner_fab_last_transaction_date.name,
        CustomerProfile.share_partner_fab_engagement_status.name,
        CustomerProfile.share_partner_costa_engagement_status.name,
        CustomerProfile.share_partner_gn_engagement_status.name,
        CustomerProfile.share_partner_etihad_engagement_status.name,
        CustomerProfile.share_partner_aljaber_engagement_status.name,
        CustomerProfile.share_partner_smiles_engagement_status.name,
        CustomerProfile.share_partner_bookingcom_engagement_status.name,
        CustomerProfile.st_mall.name,
        CustomerProfile.st_brand.name,
    ]


def get_latest_contex(pao, data_filter_context) -> str:

    latest_filter_contex = (
        pao.filter(pao.data_filter_context.like("%" + data_filter_context + "%"))
        .sort(col('date_of_prepare').desc())
        .select('data_filter_context')
        .first()[0]
    )
    print(f"Latest filter contex for {data_filter_context} : {latest_filter_contex}")

    return latest_filter_contex


def get_lifecycle(pao, data_filter_context, customer_profile_column_name) -> DataFrame:

    lifecycle = (
        pao
        .filter(col(ProfileAlgorithmsOutput.data_filter_context.name) == get_latest_contex(pao, data_filter_context))
        .select(
            ProfileAlgorithmsOutput.idi_counterparty.name,
            ProfileAlgorithmsOutput.rfm_customer_segment.name,
        )
        .withColumnRenamed(ProfileAlgorithmsOutput.rfm_customer_segment.name, customer_profile_column_name)
    ).dropDuplicates([ProfileAlgorithmsOutput.idi_counterparty.name])

    print(customer_profile_column_name)
    print(lifecycle.show(10))
    return lifecycle

def get_brand(pao, data_filter_context, purchase_propensity_column_name) -> DataFrame:

    brand = (
        pao
        .filter(col(ProfileAlgorithmsOutput.data_filter_context.name) == get_latest_contex(pao, data_filter_context))
        .select(
            ProfileAlgorithmsOutput.idi_counterparty.name,
            ProfileAlgorithmsOutput.clv_6_prob_alive.name,
        )
        .withColumnRenamed(ProfileAlgorithmsOutput.clv_6_prob_alive.name, purchase_propensity_column_name)
    ).dropDuplicates([ProfileAlgorithmsOutput.idi_counterparty.name])

    return brand


def latest_brand_output(pao) -> DataFrame:
    brand_output = pao.select(ProfileAlgorithmsOutput.idi_counterparty.name).dropDuplicates()

    brand_carrefour = get_brand(pao, 'clv.carrefour', CustomerProfile.purchase_propensity_carrefour_next_3_months.name)
    brand_share = get_brand(pao, 'clv.share', CustomerProfile.purchase_propensity_share_next_3_months.name)
    brand_skidubai = get_brand(pao, 'clv.skidubai', CustomerProfile.purchase_propensity_skidub_next_3_months.name)
    brand_magicplanet = get_brand(pao, 'clv.magicplanet', CustomerProfile.purchase_propensity_magicp_next_3_months.name)
    brand_voxcinemas = get_brand(pao, 'clv.voxcinemas', CustomerProfile.purchase_propensity_vox_next_3_months.name)
    mall_ofemirates = get_brand(pao, 'clv.ofemirates', CustomerProfile.purchase_propensity_moe_next_3_months.name)
    brand_lululemon = get_brand(pao, 'clv.lululemon', CustomerProfile.purchase_propensity_lll_next_3_months.name)
    mall_citycentredeira = get_brand(pao, 'clv.citycentredeira', CustomerProfile.purchase_propensity_ccd_next_3_months.name)
    mall_citycentremirdif = get_brand(pao, 'clv.citycentremirdif', CustomerProfile.purchase_propensity_ccmi_next_3_months.name)
    brand_allsaints = get_brand(pao, 'clv.allsaints', CustomerProfile.purchase_propensity_als_next_3_months.name)
    mall_cratebarrel = get_brand(pao, 'clv.cratebarrel', CustomerProfile.purchase_propensity_cnb_next_3_months.name)
    custom_lec = get_brand(pao, 'clv.lec', CustomerProfile.purchase_propensity_lec_next_3_months.name)
    custom_smbu = get_brand(pao, 'clv.smbu', CustomerProfile.purchase_propensity_smbu_next_3_months.name)
    custom_lifestyle = get_brand(pao, 'clv.lifestyle', CustomerProfile.purchase_propensity_lifestyle_next_3_months.name)

    custom_moeluxury = get_brand(pao, 'clv.moeluxury', CustomerProfile.purchase_propensity_moe_luxury_next_3_months.name)
    custom_moegroceries = get_brand(pao, 'clv.moegroceries', CustomerProfile.purchase_propensity_moe_groceries_next_3_months.name)
    custom_moefb = get_brand(pao, 'clv.moefb', CustomerProfile.purchase_propensity_moe_fnb_next_3_months.name)
    custom_moefashion = get_brand(pao, 'clv.moefashion', CustomerProfile.purchase_propensity_moe_fashion_accessories_next_3_months.name)
    custom_ccmiluxury = get_brand(pao, 'clv.ccmiluxury', CustomerProfile.purchase_propensity_ccmi_luxury_next_3_months.name)
    custom_ccmifashio = get_brand(pao, 'clv.ccmifashio',  CustomerProfile.purchase_propensity_ccmi_fashion_accessories_next_3_months.name)
    custom_furnitre = get_brand(pao, 'clv.ccmifurnitre', CustomerProfile.purchase_propensity_ccmi_homefurniture_electronics_next_3_months.name)
    custom_ccmibeauty = get_brand(pao, 'clv.ccmibeauty', CustomerProfile.purchase_propensity_ccmi_beauty_next_3_months.name)
    custom_ccmifb = get_brand(pao, 'clv.ccmifb', CustomerProfile.purchase_propensity_ccmi_fnb_next_3_months.name)
    custom_shareluxury = get_brand(pao, 'clv.shareluxury', CustomerProfile.purchase_propensity_luxury.name)

    lifecycle_stage_als = get_lifecycle(pao, 'rfm.als', CustomerProfile.lifecycle_stage_als.name)
    lifecycle_stage_carrefour = get_lifecycle(pao, 'rfm.carrefour', CustomerProfile.lifecycle_stage_carrefour.name)
    lifecycle_stage_ccd_rfm = get_lifecycle(pao, 'rfm.ccd', CustomerProfile.lifecycle_stage_ccd.name)
    lifecycle_stage_ccmi = get_lifecycle(pao, 'rfm.ccmi', CustomerProfile.lifecycle_stage_ccmi.name)
    lifecycle_stage_cnb = get_lifecycle(pao, 'rfm.cnb', CustomerProfile.lifecycle_stage_cnb.name)
    lifecycle_stage_crf = get_lifecycle(pao, 'rfm.carrefour', CustomerProfile.lifecycle_stage_crf.name)
    lifecycle_stage_lec = get_lifecycle(pao, 'rfm.lec', CustomerProfile.lifecycle_stage_lec.name)
    lifecycle_stage_lifestyle = get_lifecycle(pao, 'rfm.lifestyle', CustomerProfile.lifecycle_stage_lifestyle.name)
    lifecycle_stage_lll = get_lifecycle(pao, 'rfm.lll', CustomerProfile.lifecycle_stage_lll.name)
    lifecycle_stage_magicp = get_lifecycle(pao, 'rfm.magicplanet', CustomerProfile.lifecycle_stage_magicp.name)
    lifecycle_stage_moe = get_lifecycle(pao, 'rfm.moe', CustomerProfile.lifecycle_stage_moe.name)
    lifecycle_stage_share = get_lifecycle(pao, 'rfm.shareproposition1', CustomerProfile.lifecycle_stage_share.name)
    lifecycle_stage_skidub = get_lifecycle(pao, 'rfm.skidubai', CustomerProfile.lifecycle_stage_skidub.name)
    lifecycle_stage_smbu = get_lifecycle(pao, 'rfm.smbu', CustomerProfile.lifecycle_stage_smbu.name)
    lifecycle_stage_vox = get_lifecycle(pao, 'rfm.vox', CustomerProfile.lifecycle_stage_vox.name)

    brand_output = (
        brand_output
        .join(brand_carrefour, on=[ProfileAlgorithmsOutput.idi_counterparty.name], how='left')
        .join(brand_share, on=[ProfileAlgorithmsOutput.idi_counterparty.name], how='left')
        .join(brand_skidubai, on=[ProfileAlgorithmsOutput.idi_counterparty.name], how='left')
        .join(brand_magicplanet, on=[ProfileAlgorithmsOutput.idi_counterparty.name], how='left')
        .join(brand_voxcinemas, on=[ProfileAlgorithmsOutput.idi_counterparty.name], how='left')
        .join(mall_ofemirates, on=[ProfileAlgorithmsOutput.idi_counterparty.name], how='left')
        .join(brand_lululemon, on=[ProfileAlgorithmsOutput.idi_counterparty.name], how='left')
        .join(mall_citycentredeira, on=[ProfileAlgorithmsOutput.idi_counterparty.name], how='left')
        .join(mall_citycentremirdif, on=[ProfileAlgorithmsOutput.idi_counterparty.name], how='left')
        .join(brand_allsaints, on=[ProfileAlgorithmsOutput.idi_counterparty.name], how='left')
        .join(mall_cratebarrel, on=[ProfileAlgorithmsOutput.idi_counterparty.name], how='left')
        .join(custom_lec, on=[ProfileAlgorithmsOutput.idi_counterparty.name], how='left')
        .join(custom_smbu, on=[ProfileAlgorithmsOutput.idi_counterparty.name], how='left')
        .join(custom_lifestyle, on=[ProfileAlgorithmsOutput.idi_counterparty.name], how='left')

        .join(custom_moeluxury, on=[ProfileAlgorithmsOutput.idi_counterparty.name], how='left')
        .join(custom_moegroceries, on=[ProfileAlgorithmsOutput.idi_counterparty.name], how='left')
        .join(custom_moefb, on=[ProfileAlgorithmsOutput.idi_counterparty.name], how='left')
        .join(custom_moefashion, on=[ProfileAlgorithmsOutput.idi_counterparty.name], how='left')
        .join(custom_ccmiluxury, on=[ProfileAlgorithmsOutput.idi_counterparty.name], how='left')
        .join(custom_ccmifashio, on=[ProfileAlgorithmsOutput.idi_counterparty.name], how='left')
        .join(custom_furnitre, on=[ProfileAlgorithmsOutput.idi_counterparty.name], how='left')
        .join(custom_ccmibeauty, on=[ProfileAlgorithmsOutput.idi_counterparty.name], how='left')
        .join(custom_ccmifb, on=[ProfileAlgorithmsOutput.idi_counterparty.name], how='left')
        .join(custom_shareluxury, on=[ProfileAlgorithmsOutput.idi_counterparty.name], how='left')

        .join(lifecycle_stage_als, on=[ProfileAlgorithmsOutput.idi_counterparty.name], how='left')  # lifecycle
        .join(lifecycle_stage_carrefour, on=[ProfileAlgorithmsOutput.idi_counterparty.name], how='left')
        .join(lifecycle_stage_ccd_rfm, on=[ProfileAlgorithmsOutput.idi_counterparty.name], how='left')
        .join(lifecycle_stage_ccmi, on=[ProfileAlgorithmsOutput.idi_counterparty.name], how='left')
        .join(lifecycle_stage_cnb, on=[ProfileAlgorithmsOutput.idi_counterparty.name], how='left')
        .join(lifecycle_stage_crf, on=[ProfileAlgorithmsOutput.idi_counterparty.name], how='left')
        .join(lifecycle_stage_lec, on=[ProfileAlgorithmsOutput.idi_counterparty.name], how='left')
        .join(lifecycle_stage_lifestyle, on=[ProfileAlgorithmsOutput.idi_counterparty.name], how='left')
        .join(lifecycle_stage_lll, on=[ProfileAlgorithmsOutput.idi_counterparty.name], how='left')
        .join(lifecycle_stage_magicp, on=[ProfileAlgorithmsOutput.idi_counterparty.name], how='left')
        .join(lifecycle_stage_moe, on=[ProfileAlgorithmsOutput.idi_counterparty.name], how='left')
        .join(lifecycle_stage_share, on=[ProfileAlgorithmsOutput.idi_counterparty.name], how='left')
        .join(lifecycle_stage_skidub, on=[ProfileAlgorithmsOutput.idi_counterparty.name], how='left')
        .join(lifecycle_stage_smbu, on=[ProfileAlgorithmsOutput.idi_counterparty.name], how='left')
        .join(lifecycle_stage_vox, on=[ProfileAlgorithmsOutput.idi_counterparty.name], how='left')
    )

    return brand_output


def get_nbo(pao: DataFrame, sponsor_dict: DataFrame, prod04_dict: DataFrame, prod03_dict: DataFrame, prod00_dict: DataFrame) -> DataFrame:
    nbo_output = pao.select(ProfileAlgorithmsOutput.idi_counterparty.name).dropDuplicates()

    nbo_1 = (
        pao
        .filter(col(ProfileAlgorithmsOutput.data_filter_context.name) == get_latest_contex(pao, 'nbo.sharebrand'))
        .filter(col(ProfileAlgorithmsOutput.nbo_rank.name) == '1')
        .select([
            col(ProfileAlgorithmsOutput.idi_counterparty.name),
            col(ProfileAlgorithmsOutput.nbo_json.name),
        ])
        .withColumnRenamed(ProfileAlgorithmsOutput.nbo_json.name, CustomerProfile.nbo1.name)
    )

    nbo_1 = (
        nbo_1.
        join(sponsor_dict, on=sponsor_dict.sponsor_key == nbo_1.nbo1, how='left')
        .select([
            col(ProfileAlgorithmsOutput.idi_counterparty.name),
            col("sponsor_name"),
        ])
        .withColumnRenamed("sponsor_name", CustomerProfile.nbo1.name)
    )

    nbo_2 = (
        pao
        .filter(col(ProfileAlgorithmsOutput.data_filter_context.name) == get_latest_contex(pao, 'nbo.sharemall'))
        .filter(col(ProfileAlgorithmsOutput.nbo_rank.name) == '1')
        .select([
            col(ProfileAlgorithmsOutput.idi_counterparty.name),
            col(ProfileAlgorithmsOutput.nbo_json.name),
        ])
        .withColumnRenamed(ProfileAlgorithmsOutput.nbo_json.name, CustomerProfile.nbo2.name)
    )

    nbo_3 = (
        pao
        .filter(col(ProfileAlgorithmsOutput.data_filter_context.name) == get_latest_contex(pao, 'nbo.shareproposition3'))
        .filter(col(ProfileAlgorithmsOutput.nbo_rank.name) == '1')
        .select([
            col(ProfileAlgorithmsOutput.idi_counterparty.name),
            col(ProfileAlgorithmsOutput.nbo_json.name),
        ])
        .withColumnRenamed(ProfileAlgorithmsOutput.nbo_json.name, CustomerProfile.nbo3.name)
    )

    nbo_3 = (
        nbo_3.
        join(prod03_dict, on=prod03_dict.idi_proposition_level03 == nbo_3.nbo3, how='left')
        .select([
            col(ProfileAlgorithmsOutput.idi_counterparty.name),
            col("nam_proposition_level03"),
        ])
        .withColumnRenamed("nam_proposition_level03", CustomerProfile.nbo3.name)
    )

    nbo_4 = (
        pao
        .filter(col(ProfileAlgorithmsOutput.data_filter_context.name) == get_latest_contex(pao, 'nbo.shareproposition4'))
        .filter(col(ProfileAlgorithmsOutput.nbo_rank.name) == '1')
        .select([
            col(ProfileAlgorithmsOutput.idi_counterparty.name),
            col(ProfileAlgorithmsOutput.nbo_json.name),
        ])
        .withColumnRenamed(ProfileAlgorithmsOutput.nbo_json.name, CustomerProfile.nbo4.name)

    )

    nbo_4 = (
        nbo_4.
        join(prod04_dict, on=prod04_dict.idi_proposition_level04 == nbo_4.nbo4, how='left')
        .select([
            col(ProfileAlgorithmsOutput.idi_counterparty.name),
            col("nam_proposition_level04"),
        ])
        .withColumnRenamed("nam_proposition_level04", CustomerProfile.nbo4.name)
        # .withColumn(CustomerProfile.nbo5.name, lit(None).cast(StringType()))
        .withColumn(CustomerProfile.nbo6.name, lit(None).cast(StringType()))
    )

    # nbo_5 = (
    #     pao
    #     .filter(col(ProfileAlgorithmsOutput.data_filter_context.name) == get_latest_contex(pao, 'nbo.shareproposition5'))
    #     .filter(col(ProfileAlgorithmsOutput.nbo_rank.name) == '1')
    #     .select([
    #         col(ProfileAlgorithmsOutput.idi_counterparty.name),
    #         col(ProfileAlgorithmsOutput.nbo_json.name),
    #     ])
    #     .withColumnRenamed(ProfileAlgorithmsOutput.nbo_json.name, CustomerProfile.nbo5.name)
    # )


    # nbo_4 = (
    #     nbo_4.
    #     join(prod00_dict, on=prod00_dict.idi_proposition == nbo_4.nbo4, how='left')
    #     .select([
    #         col(ProfileAlgorithmsOutput.idi_counterparty.name),
    #         col("nam_proposition"),
    #     ])
    #     .withColumnRenamed("nam_proposition", CustomerProfile.nbo4.name)
    # )

    nbo_moe = (
        pao
        .filter(col(ProfileAlgorithmsOutput.data_filter_context.name) == get_latest_contex(pao, 'nbo.moe'))
        .filter(col(ProfileAlgorithmsOutput.nbo_rank.name) == '1')
        .select([
            col(ProfileAlgorithmsOutput.idi_counterparty.name),
            col(ProfileAlgorithmsOutput.nbo_json.name),
        ])
        .withColumnRenamed(ProfileAlgorithmsOutput.nbo_json.name, CustomerProfile.next_best_category_moe.name)
    )

    nbo_moe = (
        nbo_moe.
        join(prod03_dict, on=prod03_dict.idi_proposition_level03 == nbo_moe.next_best_category_moe, how='left')
        .select([
            col(ProfileAlgorithmsOutput.idi_counterparty.name),
            col("nam_proposition_level03"),
        ])
        .withColumnRenamed("nam_proposition_level03", CustomerProfile.next_best_category_moe.name)
    )

    nbo_ccmi = (
        pao
        .filter(col(ProfileAlgorithmsOutput.data_filter_context.name) == get_latest_contex(pao, 'nbo.ccmi'))
        .filter(col(ProfileAlgorithmsOutput.nbo_rank.name) == '1')
        .select([
            col(ProfileAlgorithmsOutput.idi_counterparty.name),
            col(ProfileAlgorithmsOutput.nbo_json.name),
        ])
        .withColumnRenamed(ProfileAlgorithmsOutput.nbo_json.name, CustomerProfile.next_best_category_ccmi.name)
    )

    nbo_ccmi = (
        nbo_ccmi.
        join(prod03_dict, on=prod03_dict.idi_proposition_level03 == nbo_ccmi.next_best_category_ccmi, how='left')
        .select([
            col(ProfileAlgorithmsOutput.idi_counterparty.name),
            col("nam_proposition_level03"),
        ])
        .withColumnRenamed("nam_proposition_level03", CustomerProfile.next_best_category_ccmi.name)
    )

    nbo_share = (
        pao
        .filter(col(ProfileAlgorithmsOutput.data_filter_context.name) == get_latest_contex(pao, 'nbo.shareproposition3'))
        .filter(col(ProfileAlgorithmsOutput.nbo_rank.name) == '1')
        .select([
            col(ProfileAlgorithmsOutput.idi_counterparty.name),
            col(ProfileAlgorithmsOutput.nbo_json.name),
        ])
        .withColumnRenamed(ProfileAlgorithmsOutput.nbo_json.name, CustomerProfile.next_best_category_share.name)
    )

    nbo_share = (
        nbo_share.
        join(prod03_dict, on=prod03_dict.idi_proposition_level03 == nbo_share.next_best_category_share, how='left')
        .select([
            col(ProfileAlgorithmsOutput.idi_counterparty.name),
            col("nam_proposition_level03"),
        ])
        .withColumnRenamed("nam_proposition_level03", CustomerProfile.next_best_category_share.name)
    )



    nbo_output = (
        nbo_output
        .join(nbo_1, on=[ProfileAlgorithmsOutput.idi_counterparty.name], how='left')
        .join(nbo_2, on=[ProfileAlgorithmsOutput.idi_counterparty.name], how='left')
        .join(nbo_3, on=[ProfileAlgorithmsOutput.idi_counterparty.name], how='left')
        .join(nbo_4, on=[ProfileAlgorithmsOutput.idi_counterparty.name], how='left')
        # .join(nbo_5, on=[ProfileAlgorithmsOutput.idi_counterparty.name], how='left')
        # .join(nbo_6, on=[ProfileAlgorithmsOutput.idi_counterparty.name], how='left')
        .join(nbo_moe, on=[ProfileAlgorithmsOutput.idi_counterparty.name], how='left')
        .join(nbo_ccmi, on=[ProfileAlgorithmsOutput.idi_counterparty.name], how='left')
        .join(nbo_share, on=[ProfileAlgorithmsOutput.idi_counterparty.name], how='left')

    )
    nbo_output.printSchema()
    nbo_output.show()

    return nbo_output


def run(
        demo: DataFrame, id_map: DataFrame,
        braze: DataFrame,
        rfm: DataFrame, clv: DataFrame,
        kpis: DataFrame, pao: DataFrame,
        sponsor_dict: DataFrame,
        prod04_dict: DataFrame,
        prod03_dict: DataFrame,
        prod00_dict: DataFrame,
) -> DataFrame:
    id_map = id_map.select(
        IdMapping.idi_src.name,
        IdMapping.idi_gcr.name,
        IdMapping.cod_sor_idi_src.name
    )

    demo = demo.select(*demographics_columns_for_c360)

    share_id_map = id_map.filter(col(IdMapping.cod_sor_idi_src.name) == '971005')  # get only mappings for share
    # share profiles only
    profile = (
        demo
        .filter(col(Demographics.bu.name) == 'gcr')
        .alias(Demographics.table_alias())
        .join(
            how='inner',
            other=share_id_map.alias(IdMapping.table_alias()),
            on=col(Demographics.idi_counterparty.column_alias()) == col(IdMapping.idi_gcr.column_alias())
        )
        .dropDuplicates([
            Demographics.idi_counterparty.name
        ])
        .drop(
            IdMapping.idi_gcr.name,
            # IdMapping.idi_src.name, -> required later for linking NBO
            IdMapping.cod_sor_idi_src.name
        )
    )

    profile = (
        profile
        .withColumn(colName=CustomerProfile.des_nationality.name, col=lit('empty'))
    )

    kpis = (
        kpis.withColumnRenamed("idi_counterparty_gr", CustomerProfile.idi_counterparty.name)
            .withColumn(ProfileCalculatedKpi.partner_first_transaction_date.name, col(ProfileCalculatedKpi.partner_first_transaction_date.name).cast(DateType()))
            .withColumn(ProfileCalculatedKpi.partner_last_transaction_date.name, col(ProfileCalculatedKpi.partner_last_transaction_date.name).cast(DateType()))
    )

    # enrich  with KPIs
    profile = (
        profile
        .join(kpis, on=[CustomerProfile.idi_counterparty.name], how='left')
        .dropDuplicates([
            CustomerProfile.idi_counterparty.name
        ])
    )

    # empty
    profile = (
        profile
        # .withColumn(CustomerProfile.purchase_propensity_luxury.name, lit(None).cast(FloatType()))
        # .withColumn(CustomerProfile.purchase_propensity_moe_luxury_next_3_months.name, lit(None).cast(FloatType()))
        # .withColumn(CustomerProfile.purchase_propensity_moe_groceries_next_3_months.name, lit(None).cast(FloatType()))
        # .withColumn(CustomerProfile.purchase_propensity_moe_fnb_next_3_months.name, lit(None).cast(FloatType()))
        # .withColumn(CustomerProfile.purchase_propensity_moe_fashion_accessories_next_3_months.name, lit(None).cast(FloatType()))
        # .withColumn(CustomerProfile.purchase_propensity_ccmi_luxury_next_3_months.name, lit(None).cast(FloatType()))
        # .withColumn(CustomerProfile.purchase_propensity_ccmi_fashion_accessories_next_3_months.name, lit(None).cast(FloatType()))
        # .withColumn(CustomerProfile.purchase_propensity_ccmi_homefurniture_electronics_next_3_months.name, lit(None).cast(FloatType()))
        # .withColumn(CustomerProfile.purchase_propensity_ccmi_beauty_next_3_months.name, lit(None).cast(FloatType()))
        # .withColumn(CustomerProfile.purchase_propensity_ccmi_fnb_next_3_months.name, lit(None).cast(FloatType()))
        .withColumn(CustomerProfile.next_best_share_partners.name, lit(None).cast(StringType()))
        # .withColumn(CustomerProfile.next_best_category_moe.name, lit(None).cast(StringType()))
        # .withColumn(CustomerProfile.next_best_category_ccmi.name, lit(None).cast(StringType()))
        # .withColumn(CustomerProfile.next_best_category_share.name, lit(None).cast(StringType()))
        .withColumn(CustomerProfile.next_best_category_lifestyle.name, lit(None).cast(StringType()))
        .withColumn(CustomerProfile.next_best_category_that.name, lit(None).cast(StringType()))
        .withColumn(CustomerProfile.next_best_category_cnb.name, lit(None).cast(StringType()))
        .withColumn(CustomerProfile.next_best_category_lll.name, lit(None).cast(StringType()))
        .withColumn(CustomerProfile.next_best_category_lego.name, lit(None).cast(StringType()))
        .withColumn(CustomerProfile.next_best_category_als.name, lit(None).cast(StringType()))
        .withColumn(CustomerProfile.next_best_category_cb2.name, lit(None).cast(StringType()))
        .withColumn(CustomerProfile.next_best_category_anf.name, lit(None).cast(StringType()))
        .withColumn(CustomerProfile.next_best_category_hco.name, lit(None).cast(StringType()))
        .withColumn(CustomerProfile.next_best_category_shi.name, lit(None).cast(StringType()))
        .withColumn(CustomerProfile.next_best_category_pf.name, lit(None).cast(StringType()))
        .withColumn(CustomerProfile.next_best_product_lifestyle_by_brand.name, lit(None).cast(StringType()))
        # .withColumn(CustomerProfile.experiences.name, lit(None).cast(StringType()))
        # .withColumn(CustomerProfile.category_group.name, lit(None).cast(StringType()))
        # .withColumn(CustomerProfile.store_category.name, lit(None).cast(StringType()))

    )

    # enrich with brand
    brand_output = latest_brand_output(pao)

    profile = (
        profile
        .join(brand_output, on=[CustomerProfile.idi_counterparty.name], how='left')
        .dropDuplicates([
            CustomerProfile.idi_counterparty.name
        ])
    )

    # enrich w NBO
    nbo_1_6 = get_nbo(pao, sponsor_dict,  prod04_dict, prod03_dict, prod00_dict)

    profile = (
        profile
        .join(nbo_1_6, on=[CustomerProfile.idi_counterparty.name], how='left')
        .dropDuplicates([
            CustomerProfile.idi_counterparty.name
        ])
    )

    # enrich clv
    clv_alias = features.ClvOutput.table_alias()

    profile = (
        profile.alias(CustomerProfile.table_alias())
        .join(clv.alias(clv_alias), on=[CustomerProfile.idi_counterparty.name], how='left')
        # .drop(col(features.ClvOutput.idi_counterparty.column_alias()))
        .withColumn(CustomerProfile.min_date.name, lit(None).cast(DateType())) #TODO
        .withColumn(CustomerProfile.max_date.name, lit(None).cast(DateType()))
        .withColumn(
            CustomerProfile.frequency.name,
            col(f"{clv_alias}.FREQUENCY").cast(IntegerType())
        ).drop(f"{clv_alias}.FREQUENCY")
        .withColumn(
            CustomerProfile.monetary.name,
            col(f"{clv_alias}.MONETARY").cast(FloatType())
        ).drop(f"{clv_alias}.MONETARY")
        .withColumn(
            CustomerProfile.customer_age.name,
            col(f"{clv_alias}.T").cast(LongType())
        ).drop(f"{clv_alias}.T")
        .withColumn(
            CustomerProfile.expected_purchases_3m.name,
            col(f"{clv_alias}.6_MONTHS_EXPECTED_PURCHASES").cast(FloatType())
        ).drop(f"{clv_alias}.6_MONTHS_EXPECTED_PURCHASES")
        .withColumn(
            CustomerProfile.clv_3m.name,
            col(f"{clv_alias}.6_MONTHS_CLV").cast(FloatType())
        ).drop(f"{clv_alias}.6_MONTHS_CLV")
        .withColumn(
            CustomerProfile.p_alive.name,
            col(f"{clv_alias}.PROB_ALIVE").cast(FloatType())
        ).drop(f"{clv_alias}.PROB_ALIVE")
        .dropDuplicates([
            CustomerProfile.idi_counterparty.name
        ])
    )

    # enrich w rfm
    profile = (
        profile.alias(CustomerProfile.table_alias())
        .join(
            how='left',
            other=rfm.alias(features.RfmOutput.table_alias()),
            on=col(
                CustomerProfile.idi_counterparty.column_alias()) == col(
                features.RfmOutput.idi_counterparty.column_alias()
            )
        )
        .drop(col(features.RfmOutput.idi_counterparty.column_alias()))
        .drop(col(features.RfmOutput.idi_proposition_level03.column_alias()))
        .drop(col(CustomerProfile.lifecycle_stage_share.column_alias()))  # changed
        .withColumn(
            CustomerProfile.lifecycle_stage_share.name,
            col(features.RfmOutput.customer_segment.column_alias())
        )  # changed
        .withColumn(
            CustomerProfile.recency_proposition_level_3.name,
            col(features.RfmOutput.recency.column_alias()).cast(IntegerType())
        ).drop(col(features.RfmOutput.recency.column_alias()))
        .withColumn(
            CustomerProfile.frequency_proposition_level_3.name,
            col(features.RfmOutput.frequency.column_alias()).cast(IntegerType())
        ).drop(col(features.RfmOutput.frequency.column_alias()))
        .withColumn(
            CustomerProfile.monetary_proposition_level_3.name,
            col(features.RfmOutput.monetary.column_alias()).cast(FloatType())
        ).drop(col(features.RfmOutput.monetary.column_alias()))
        .withColumn(
            CustomerProfile.r_rank_norm_proposition_level_3.name,
            col(features.RfmOutput.r_rank_norm.column_alias()).cast(FloatType())
        ).drop(col(features.RfmOutput.r_rank_norm.column_alias()))
        .withColumn(
            CustomerProfile.f_rank_norm_proposition_level_3.name,
            col(features.RfmOutput.f_rank_norm.column_alias()).cast(FloatType())
        ).drop(col(features.RfmOutput.f_rank_norm.column_alias()))
        .withColumn(
            CustomerProfile.m_rank_norm_proposition_level_3.name,
            col(features.RfmOutput.m_rank_norm.column_alias()).cast(FloatType())
        ).drop(col(features.RfmOutput.m_rank_norm.column_alias()))
        .withColumnRenamed(
            existing=features.RfmOutput.customer_segment.name,
            new=CustomerProfile.customer_segment_proposition_level_3.name
        )
        .dropDuplicates([
            CustomerProfile.idi_counterparty.name
        ])
    )

    profile = profile.withColumn(
        CustomerProfile.r_rank_norm_proposition_level_3.name,
        when(col(CustomerProfile.r_rank_norm_proposition_level_3.name) == 1, "Least Recent")
        .when(col(CustomerProfile.r_rank_norm_proposition_level_3.name) == 2, "Less Recent")
        .when(col(CustomerProfile.r_rank_norm_proposition_level_3.name) == 3, "Average")
        .when(col(CustomerProfile.r_rank_norm_proposition_level_3.name) == 4, "More Recent")
        .when(col(CustomerProfile.r_rank_norm_proposition_level_3.name) == 5, "Most Recent")
        .otherwise("None")
    )

    profile = profile.withColumn(
        CustomerProfile.f_rank_norm_proposition_level_3.name,
        when(col(CustomerProfile.f_rank_norm_proposition_level_3.name) == 1, "Least Frequent")
        .when(col(CustomerProfile.f_rank_norm_proposition_level_3.name) == 2, "Less Frequent")
        .when(col(CustomerProfile.f_rank_norm_proposition_level_3.name) == 3, "Average")
        .when(col(CustomerProfile.f_rank_norm_proposition_level_3.name) == 4, "More Frequent")
        .when(col(CustomerProfile.f_rank_norm_proposition_level_3.name) == 5, "Most Frequent")
        .otherwise("None")
    )

    profile = profile.withColumn(
        CustomerProfile.m_rank_norm_proposition_level_3.name,
        when(col(CustomerProfile.m_rank_norm_proposition_level_3.name) == 1, "Very Low Value")
        .when(col(CustomerProfile.m_rank_norm_proposition_level_3.name) == 2, "Low Value")
        .when(col(CustomerProfile.m_rank_norm_proposition_level_3.name) == 3, "Average")
        .when(col(CustomerProfile.m_rank_norm_proposition_level_3.name) == 4, "High Value")
        .when(col(CustomerProfile.m_rank_norm_proposition_level_3.name) == 5, "Very High Value")
        .otherwise("None")
    )

    profile = convert_columns_to_lower_case(profile)

    profile = (
        profile
        .join(braze, on=[CustomerProfile.idi_counterparty.name], how='left')
        .dropDuplicates([
            CustomerProfile.idi_counterparty.name
        ])
    )

    # Map Braze attributes
    profile = (
        profile
        .withColumnRenamed(
            CustomAttributes.vox_total_spend_for_12_months_164.name,
            CustomerProfile.vox_total_spend_for_12_months.name
        )
        .withColumnRenamed(
            CustomAttributes.vox_total_number_transactions_191.name,
            CustomerProfile.vox_total_number_transactions.name
        )
        .withColumnRenamed(
            CustomAttributes.vox_preferred_mall_237.name,
            CustomerProfile.vox_preferred_mall.name
        )
        .withColumnRenamed(
            CustomAttributes.vox_number_movies_last_1_month_245.name,
            CustomerProfile.vox_number_movies_last_1_month.name
        )
        .withColumnRenamed(
            CustomAttributes.vox_most_recent_movie_watched_244.name,
            CustomerProfile.vox_most_recent_movie_watched.name
        )
        .withColumnRenamed(
            CustomAttributes.vox_kids_movie_total_txn_236.name,
            CustomerProfile.vox_kids_movie_total_txn.name
        )
        .withColumnRenamed(
            CustomAttributes.vox_hindi_movie_count_170.name,
            CustomerProfile.vox_hindi_movie_count.name
        )
        .withColumnRenamed(
            CustomAttributes.vox_family_movie_total_txn_214.name,
            CustomerProfile.vox_family_movie_total_txn.name
        )
        .withColumnRenamed(
            CustomAttributes.vox_family_movie_total_revenue_199.name,
            CustomerProfile.vox_family_movie_total_revenue.name
        )
        .withColumnRenamed(
            CustomAttributes.vox_cinema_total_spend_last_6_months_176.name,
            CustomerProfile.vox_cinema_total_spend_last_6_months.name
        )
        .withColumnRenamed(
            CustomAttributes.vox_cinema_total_spend_last_3_months_177.name,
            CustomerProfile.vox_cinema_total_spend_last_3_months.name
        )
        .withColumnRenamed(
            CustomAttributes.vox_cinema_total_spend_last_12_months_175.name,
            CustomerProfile.vox_cinema_total_spend_last_12_months.name
        )
        .withColumnRenamed(
            CustomAttributes.vox_childrens_movie_count_172.name,
            CustomerProfile.vox_children_movie_count.name
        )
        .withColumnRenamed(
            CustomAttributes.vox_avg_txn_value_for_12_months_159.name,
            CustomerProfile.vox_avg_txn_value_for_12_months.name
        )
        .withColumnRenamed(
            CustomAttributes.vox_arabic_movie_total_txn_231.name,
            CustomerProfile.vox_arabic_movie_total_txn.name
        )
        .withColumnRenamed(
            CustomAttributes.vox_arabic_movie_count_169.name,
            CustomerProfile.vox_arabic_movie_count.name
        )
        .withColumnRenamed(
            CustomAttributes.sw_segment_code.name,
            CustomerProfile.sw_segment_code.name
        )
        .withColumnRenamed(
            CustomAttributes.sw_mall_status.name,
            CustomerProfile.sw_mall_status.name
        )
        .withColumn(
            CustomerProfile.sw_mall_last_bit_date.name,
            to_date(CustomAttributes.sw_mall_last_bit_date.name),
        )
        .withColumnRenamed(
            CustomAttributes.sw_lifestyle_status.name,
            CustomerProfile.sw_lifestyle_status.name
        )
        .withColumn(
            CustomerProfile.sw_lifestyle_last_bit_date.name,
            to_date(CustomAttributes.sw_lifestyle_last_bit_date.name)
        )
        .withColumnRenamed(
            CustomAttributes.sw_l_and_e_status.name,
            CustomerProfile.sw_l_and_e_status.name
        )
        .withColumn(
            CustomerProfile.sw_l_and_e_last_bit_date.name,
            to_date(CustomAttributes.sw_l_and_e_last_bit_date.name)
        )
        .withColumnRenamed(
            CustomAttributes.sw_hotel_status.name,
            CustomerProfile.sw_hotel_status.name
        )
        .withColumn(
            CustomerProfile.sw_hotel_last_bit_date.name,
            to_date(CustomAttributes.sw_hotel_last_bit_date.name),
        )
        .withColumnRenamed(
            CustomAttributes.sw_grocery_status.name,
            CustomerProfile.sw_grocery_status.name
        )
        .withColumn(
            CustomerProfile.sw_grocery_last_bit_date.name,
            to_date(CustomAttributes.sw_grocery_last_bit_date.name)
        )
        .withColumnRenamed(
            CustomAttributes.sw_completion_status.name,
            CustomerProfile.sw_completion_status.name
        )
        .withColumnRenamed(
            CustomAttributes.ski_total_txn_last_6_months_301.name,
            CustomerProfile.ski_total_txn_last_6_months.name
        )
        .withColumnRenamed(
            CustomAttributes.ski_total_txn_last_3_months_302.name,
            CustomerProfile.ski_total_txn_last_3_months.name
        )
        .withColumnRenamed(
            CustomAttributes.share_active_90_days_577.name,
            CustomerProfile.share_active_90_days.name
        )
        .withColumnRenamed(
            CustomAttributes.share_active_60_days_576.name,
            CustomerProfile.share_active_60_days.name
        )
        .withColumnRenamed(
            CustomAttributes.share_active_30_days_575.name,
            CustomerProfile.share_active_30_days.name
        )
        .withColumnRenamed(
            CustomAttributes.share_active_180_days_578.name,
            CustomerProfile.share_active_180_days.name
        )
        .withColumnRenamed(
            CustomAttributes.registered_from.name,
            CustomerProfile.registered_from.name
        )
        .withColumn(
            CustomerProfile.qty_redeemed_total.name,
            col(CustomAttributes.qty_redeemed_total.name).cast(FloatType())
        )
        .withColumn(
            CustomerProfile.qty_points_won_spin_wheel.name,
            col(CustomAttributes.qty_points_won_spin_wheel.name).cast(FloatType()),
        )
        .withColumn(
            CustomerProfile.qty_points_balance.name,
            col(CustomAttributes.qty_accrued_total.name).cast(FloatType()),
        )
        .withColumn(
            CustomerProfile.qty_accrued_total.name,
            col(CustomAttributes.qty_accrued_total.name).cast(FloatType()),
        )
        .withColumn(
            CustomerProfile.offline_first_maf_contact.name,
            to_date(CustomAttributes.offline_first_maf_contact.name)
        )
        .withColumnRenamed(
            CustomAttributes.nam_title.name,
            CustomerProfile.nam_title.name
        )
        .withColumnRenamed(
            CustomAttributes.nam_enrollment_sponsor.name,
            CustomerProfile.nam_enrollment_sponsor.name
        )
        .withColumnRenamed(
            CustomAttributes.nam_country.name,
            CustomerProfile.nam_country.name
        )
        .withColumnRenamed(
            CustomAttributes.nam_brand.name,
            CustomerProfile.nam_brand.name
        )
        .withColumnRenamed(
            CustomAttributes.maf_cinema_total_txn_last_6_months_181.name,
            CustomerProfile.maf_cinema_total_txn_last_6_months.name
        )
        .withColumnRenamed(
            CustomAttributes.maf_cinema_total_txn_last_3_months_182.name,
            CustomerProfile.maf_cinema_total_txn_last_3_months.name
        )
        .withColumnRenamed(
            CustomAttributes.maf_cinema_total_txn_last_12_months_180.name,
            CustomerProfile.maf_cinema_total_txn_last_12_months.name
        )
        .withColumnRenamed(
            CustomAttributes.lne_total_trransactions_last_3_months_259.name,
            CustomerProfile.lne_total_transactions_last_3_months.name
        )
        .withColumnRenamed(
            CustomAttributes.lne_total_transactions_last_6_months_258.name,
            CustomerProfile.lne_total_transactions_last_6_months.name
        )
        .withColumnRenamed(
            CustomAttributes.lne_total_spend_for_12_months_250.name,
            CustomerProfile.lne_total_spend_for_12_months.name
        )
        .withColumnRenamed(
            CustomAttributes.lne_no_txn_for_12_months_247.name,
            CustomerProfile.lne_no_txn_for_12_months.name
        )
        .withColumnRenamed(
            CustomerProfile.lne_avg_txn_for_12_months_267.name,
            CustomAttributes.lne_avg_txn_for_12_months_267.name,
        )
        .withColumnRenamed(
            CustomAttributes.lne_avg_txn_for_12_months_246.name,
            CustomerProfile.lne_avg_txn_for_12_months_246.name
        )
        .withColumn(
            CustomerProfile.lifestyle_total_txn_last_last_6_months.name,
            col(CustomAttributes.lifestyle_total_txn_last_last_6_months_385.name).cast(FloatType()),
        )
        .withColumn(
            CustomerProfile.lifestyle_kids_category_spend_last_7_days.name,
            col(CustomAttributes.lifestyle_kids_category_spend_last_7_days_688.name).cast(FloatType()),
        )
        .withColumn(
            CustomerProfile.lifestyle_kids_category_spend_last_3_months.name,
            col(CustomAttributes.lifestyle_kids_category_spend_last_3_months_686.name).cast(FloatType()),
        )
        .withColumn(
            CustomerProfile.lifestyle_kids_category_spend_last_1_month.name,
            col(CustomAttributes.lifestyle_kids_category_spend_last_1_month_687.name).cast(FloatType()),
        )
        .withColumn(
            CustomerProfile.lifestyle_furniture_category_spend_last_7_days.name,
            col(CustomAttributes.lifestyle_furniture_category_spend_last_7_days_676.name).cast(FloatType()),
        )
        .withColumn(
            CustomerProfile.lifestyle_furniture_category_spend_last_3_months.name,
            col(CustomAttributes.lifestyle_furniture_category_spend_last_3_months_674.name).cast(FloatType()),
        )
        .withColumn(
            CustomerProfile.lifestyle_furniture_category_spend_last_1_month.name,
            col(CustomAttributes.lifestyle_furniture_category_spend_last_1_month_675.name).cast(FloatType()),
        )
        .withColumn(
            CustomerProfile.lifestyle_decor_category_spend_last_7_days.name,
            col(CustomAttributes.lifestyle_decor_category_spend_last_7_days_682.name).cast(FloatType()),
        )
        .withColumn(
            CustomerProfile.lifestyle_decor_category_spend_last_3_months.name,
            col(CustomAttributes.lifestyle_decor_category_spend_last_3_months_680.name).cast(FloatType()),
        )
        .withColumn(
            CustomerProfile.lifestyle_decor_category_spend_last_1_month.name,
            col(CustomAttributes.lifestyle_decor_category_spend_last_1_month_681.name).cast(FloatType()),
        )
        .withColumn(
            CustomerProfile.lifestyle_avg_spend_last_6_months.name,
            col(CustomAttributes.lifestyle_avg_spend_last_6_months_389.name).cast(FloatType()),
        )
        .withColumn(
            CustomerProfile.lifestyle_avg_spend_last_3_months.name,
            col(CustomAttributes.lifestyle_avg_spend_last_3_months_390.name).cast(FloatType()),
        )
        .withColumn(
            CustomerProfile.lifestyle_avg_spend_last_2_months.name,
            col(CustomAttributes.lifestyle_avg_spend_last_2_months_391.name).cast(FloatType()),
        )
        .withColumn(
            CustomerProfile.lifestyle_avg_spend_last_1_month.name,
            col(CustomAttributes.lifestyle_avg_spend_last_1_month_392.name).cast(FloatType()),
        )
        .withColumn(
            CustomerProfile.lec_total_spend_for_12_months.name,
            col(CustomAttributes.lec_total_spend_for_12_months_271.name).cast(FloatType()),
        )
        .withColumn(
            CustomerProfile.lec_mp_avg_txn_value_for_12_months.name,
            col(CustomAttributes.lec_mp_avg_txn_value_for_12_months_289.name).cast(FloatType()),
        )
        .withColumn(
            CustomerProfile.isd_code.name,
            col(CustomAttributes.isd_code.name).cast(StringType()),
        )
        .withColumn(
            CustomerProfile.ind_push_enabled.name,
            lit(None).cast(StringType())
        )
        .withColumnRenamed(
            "ind_coupon_generated",
            CustomerProfile.ind_coupon_generated.name
        )
        .withColumnRenamed(
            CustomAttributes.idi_counterparty.name,
            CustomerProfile.idi_counterparty.name
        )
        .withColumnRenamed(
            "ide_cc_mall_code",
            CustomerProfile.ide_cc_mall_code.name
        )
        .withColumnRenamed(
            CustomAttributes.gulf_news.name,
            CustomerProfile.gulf_news.name
        )
        .withColumnRenamed(
            CustomAttributes.fab_cobrand_product.name,
            CustomerProfile.fab_cobrand_product.name
        )
        .withColumn(
            CustomerProfile.des_summer_activity.name,
            col(CustomAttributes.des_summer_activity.name).cast(StringType()),
        )
        .withColumnRenamed(
            CustomAttributes.des_nationality.name,
            CustomerProfile.des_nationality.name
        )
        .withColumnRenamed(
            CustomAttributes.des_membership_stage.name,
            CustomerProfile.des_membership_stage.name
        )
        .withColumnRenamed(
            CustomAttributes.des_language_customer.name,
            CustomerProfile.des_language_customer.name
        )
        .withColumnRenamed(
            CustomAttributes.des_language.name,
            CustomerProfile.des_language.name
        )
        .withColumn(
            CustomerProfile.dat_registration_share.name,
            to_date(CustomAttributes.dat_registration_share.name)
        )
        .withColumn(
            CustomerProfile.dat_registration.name,
            lit(None).cast(DateType()),
        )
        .withColumn(
            CustomerProfile.dat_last_redemption.name,
            to_date(CustomAttributes.dat_last_redemption.name)
        )
        .withColumn(
            CustomerProfile.dat_last_login.name,
            to_date(CustomAttributes.dat_last_login.name)
        )
        .withColumn(
            CustomerProfile.dat_last_activity.name,
            to_date(CustomAttributes.dat_last_activity.name)
        )
        .withColumn(
            CustomerProfile.dat_last_accrual.name,
            to_date(CustomAttributes.dat_last_accrual.name)
        )
        .withColumn(
            CustomerProfile.dat_first_login_share.name,
            to_date(CustomAttributes.dat_first_login_share.name)
        )
        .withColumn(
            CustomerProfile.dat_first_login.name,
            to_date(CustomAttributes.dat_first_login.name)
        )
        .withColumn(
            CustomerProfile.dat_enrollment.name,
            to_date(CustomAttributes.dat_enrollment.name)
        )
        # .withColumn(
        #     CustomAttributes.dat_birth.name,
        #     to_date(CustomerProfile.dat_birth.name)
        # )
        # .withColumnRenamed(
        #     CustomAttributes.cod_sor_counterparty.name,
        #     CustomerProfile.cod_sor_counterparty.name
        # )
        .withColumnRenamed(
            CustomAttributes.cde_mal.name,
            CustomerProfile.cde_mal.name
        )
        .withColumnRenamed(
            CustomAttributes.cde_country_of_residence.name,
            CustomerProfile.cde_country_of_residence.name
        )
        .withColumnRenamed(
            CustomAttributes.app_id.name,
            CustomerProfile.app_id.name
        )
    )

    # apply bulk casting
    for c, t in profile.dtypes:
        if c in CustomerProfile:
            col_spec = cast(ColumnSpec, CustomerProfile[c])
            if col_spec.data_type == 'float' and t == 'string':
                profile = profile.withColumn(c, col(c).cast(FloatType()))
            elif col_spec.data_type == 'bigint' and t == 'string':
                profile = profile.withColumn(c, col(c).cast(LongType()))
            elif col_spec.data_type == 'float' and t != 'float':
                profile = profile.withColumn(c, col(c).cast(FloatType()))

    profile = (
        profile
        .drop(IdMapping.idi_src.name)
        .withColumnRenamed(IdMapping.idi_gcr.name, 'id')
        .dropDuplicates([CustomerProfile.idi_counterparty.name])
        .withColumn('account_id', col(CustomerProfile.idi_counterparty.name))
    )

    profile.printSchema()
    profile.show(5, truncate=False)

    print(len(profile.columns))

    kpis_columns = [
        CustomerProfile.sow.name,
        CustomerProfile.sow_moe.name,
        CustomerProfile.sow_cc_d.name,
        CustomerProfile.pta.name,
        CustomerProfile.sta.name,
        CustomerProfile.turnover_6m.name,
        CustomerProfile.turnover_3m.name,
        CustomerProfile.turnover_12m.name,
        CustomerProfile.turnover_3m_ni.name,
        CustomerProfile.turnover_3m_moe.name,
        CustomerProfile.engagement_status.name,
        CustomerProfile.bu_turnover.name,
        CustomerProfile.ni_locations.name,
        CustomerProfile.number_of_visits_12m.name,
        CustomerProfile.total_spend_als_last_3_months.name,
        CustomerProfile.total_spend_als_last_6_months.name,
        CustomerProfile.total_spend_carrefour_last_3_months.name,
        CustomerProfile.total_spend_carrefour_last_6_months.name,
        CustomerProfile.total_spend_ccd_last_3_months.name,
        CustomerProfile.total_spend_ccd_last_6_months.name,
        CustomerProfile.total_spend_ccmi_last_3_months.name,
        CustomerProfile.total_spend_ccmi_last_6_months.name,
        CustomerProfile.total_spend_cnb_last_3_months.name,
        CustomerProfile.total_spend_cnb_last_6_months.name,
        CustomerProfile.total_spend_crf_last_3_months.name,
        CustomerProfile.total_spend_crf_last_6_months.name,
        CustomerProfile.total_spend_lec_last_3_months.name,
        CustomerProfile.total_spend_lec_last_6_months.name,
        CustomerProfile.total_spend_lifestyle_last_3_months.name,
        CustomerProfile.total_spend_lifestyle_last_6_months.name,
        CustomerProfile.total_spend_lll_last_3_months.name,
        CustomerProfile.total_spend_lll_last_6_months.name,
        CustomerProfile.total_spend_magicp_last_3_months.name,
        CustomerProfile.total_spend_moe_last_12_months.name,
        CustomerProfile.total_spend_moe_last_1_day.name,
        CustomerProfile.total_spend_moe_last_1_month.name,
        CustomerProfile.total_spend_moe_last_3_months.name,
        CustomerProfile.total_spend_moe_last_6_months.name,
        CustomerProfile.total_spend_moe_last_7_days.name,
        CustomerProfile.total_spend_share_last_3_months.name,
        CustomerProfile.total_spend_share_last_6_months.name,
        CustomerProfile.total_spend_share_last_7_days.name,
        CustomerProfile.total_spend_skidub_last_3_months.name,
        CustomerProfile.total_spend_skidub_last_6_months.name,
        CustomerProfile.total_spend_smbu_last_3_months.name,
        CustomerProfile.total_spend_smbu_last_6_months.name,
        CustomerProfile.total_spend_vox_last_3_months.name,
        CustomerProfile.total_spend_vox_last_6_months.name,
    ]

    target_cols = {
        *demographics_columns_for_c360, *nbo_output_columns_for_360, *new_columns_fro_360, *rfm_columns, *clv_columns_for_c360, *braze_columns_for_c360, *kpis_columns, *brand_level_for_c360
    }

    cols_to_drop = set(profile.columns) - target_cols
    profile = profile.drop(*cols_to_drop)

    print(set(profile.columns))
    print(target_cols)
    print(cols_to_drop)

    # new atrribute CustomerProfile.counterparty_role 25.04.2023
    profile = profile.withColumnRenamed(Demographics.cdi_counterparty_role.name, CustomerProfile.counterparty_role.name)

    # repalace or remove ','  05.05.2021,
    profile = profile.withColumn('PTA', regexp_replace('PTA', ',', ' '))
    profile = profile.withColumn('STA', regexp_replace('STA', ',', ' '))
    profile = profile.withColumn('CDE_MAL', regexp_replace('CDE_MAL', ',', ' '))
    profile = profile.withColumn('NAM_BRAND', regexp_replace('NAM_BRAND', ',', ''))

    # new atrribute CustomerProfile.dat_birth 26.07.2023
    profile = profile.withColumnRenamed(Demographics.dat_of_birth.name, CustomerProfile.dat_birth.name)

    return profile


if __name__ == '__main__':
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from delta.tables import DeltaTable

    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'lake_descriptor', 'cvm_environment'])

    sc = SparkContext()
    glueContext = GlueContext(sc)
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    spark: SparkSession = glueContext.spark_session

    lake_descriptor = args['lake_descriptor']
    environment = args['cvm_environment']

    conf_demographics = DeltaTable.forPath(spark, get_s3_path(Demographics, lake_descriptor)).toDF()
    conf_id_map = DeltaTable.forPath(spark, get_s3_path(IdMapping, lake_descriptor)).toDF()
    conf_braze = DeltaTable.forPath(spark, get_s3_path(CustomAttributes, lake_descriptor)).toDF()

    conf_braze_max = conf_braze.groupBy('idi_counterparty').agg(
        max(col('vox_total_spend_for_12_months_164')).alias('vox_total_spend_for_12_months_164'),
        max(col('vox_total_number_transactions_191')).alias('vox_total_number_transactions_191'),
        max(col('vox_preferred_mall_237')).alias('vox_preferred_mall_237'),
        max(col('vox_number_movies_last_1_month_245')).alias('vox_number_movies_last_1_month_245'),
        max(col('vox_most_recent_movie_watched_244')).alias('vox_most_recent_movie_watched_244'),
        max(col('vox_kids_movie_total_txn_236')).alias('vox_kids_movie_total_txn_236'),
        max(col('vox_hindi_movie_count_170')).alias('vox_hindi_movie_count_170'),
        max(col('vox_family_movie_total_txn_214')).alias('vox_family_movie_total_txn_214'),
        max(col('vox_family_movie_total_revenue_199')).alias('vox_family_movie_total_revenue_199'),
        max(col('vox_cinema_total_spend_last_6_months_176')).alias('vox_cinema_total_spend_last_6_months_176'),
        max(col('vox_cinema_total_spend_last_3_months_177')).alias('vox_cinema_total_spend_last_3_months_177'),
        max(col('vox_cinema_total_spend_last_12_months_175')).alias('vox_cinema_total_spend_last_12_months_175'),
        max(col('vox_childrens_movie_count_172')).alias('vox_childrens_movie_count_172'),
        max(col('vox_avg_txn_value_for_12_months_159')).alias('vox_avg_txn_value_for_12_months_159'),
        max(col('vox_arabic_movie_total_txn_231')).alias('vox_arabic_movie_total_txn_231'),
        max(col('vox_arabic_movie_count_169')).alias('vox_arabic_movie_count_169'),
        func.concat_ws(" ", func.collect_list("sw_segment_code")).alias("sw_segment_code"),
        max(col('sw_mall_status')).alias('sw_mall_status'),
        max(col('sw_mall_last_bit_date')).alias('sw_mall_last_bit_date'),
        max(col('sw_lifestyle_status')).alias('sw_lifestyle_status'),
        max(col('sw_lifestyle_last_bit_date')).alias('sw_lifestyle_last_bit_date'),
        max(col('sw_l_and_e_status')).alias('sw_l_and_e_status'),
        max(col('sw_l_and_e_last_bit_date')).alias('sw_l_and_e_last_bit_date'),
        max(col('sw_hotel_status')).alias('sw_hotel_status'),
        max(col('sw_hotel_last_bit_date')).alias('sw_hotel_last_bit_date'),
        max(col('sw_grocery_status')).alias('sw_grocery_status'),
        max(col('sw_grocery_last_bit_date')).alias('sw_grocery_last_bit_date'),
        max(col('sw_completion_status')).alias('sw_completion_status'),
        max(col('ski_total_txn_last_6_months_301')).alias('ski_total_txn_last_6_months_301'),
        max(col('ski_total_txn_last_3_months_302')).alias('ski_total_txn_last_3_months_302'),
        max(col('share_active_90_days_577')).alias('share_active_90_days_577'),
        max(col('share_active_60_days_576')).alias('share_active_60_days_576'),
        max(col('share_active_30_days_575')).alias('share_active_30_days_575'),
        max(col('share_active_180_days_578')).alias('share_active_180_days_578'),
        max(col('registered_from')).alias('registered_from'),
        max(col('qty_redeemed_total')).alias('qty_redeemed_total'),
        max(col('qty_points_won_spin_wheel')).alias('qty_points_won_spin_wheel'),
        max(col('qty_accrued_total')).alias('qty_accrued_total'),
        max(col('offline_first_maf_contact')).alias('offline_first_maf_contact'),
        max(col('nam_title')).alias('nam_title'),
        max(col('nam_enrollment_sponsor')).alias('nam_enrollment_sponsor'),
        max(col('nam_country')).alias('nam_country'),
        max(col('nam_brand')).alias('nam_brand'),
        max(col('maf_cinema_total_txn_last_6_months_181')).alias('maf_cinema_total_txn_last_6_months_181'),
        max(col('maf_cinema_total_txn_last_3_months_182')).alias('maf_cinema_total_txn_last_3_months_182'),
        max(col('maf_cinema_total_txn_last_12_months_180')).alias('maf_cinema_total_txn_last_12_months_180'),
        max(col('lne_total_trransactions_last_3_months_259')).alias('lne_total_trransactions_last_3_months_259'),
        max(col('lne_total_transactions_last_6_months_258')).alias('lne_total_transactions_last_6_months_258'),
        max(col('lne_total_spend_for_12_months_250')).alias('lne_total_spend_for_12_months_250'),
        max(col('lne_no_txn_for_12_months_247')).alias('lne_no_txn_for_12_months_247'),
        max(col('lne_avg_txn_for_12_months_267')).alias('lne_avg_txn_for_12_months_267'),
        max(col('lne_avg_txn_for_12_months_246')).alias('lne_avg_txn_for_12_months_246'),
        max(col('lifestyle_total_txn_last_last_6_months_385')).alias('lifestyle_total_txn_last_last_6_months_385'),
        max(col('lifestyle_kids_category_spend_last_7_days_688')).alias(
            'lifestyle_kids_category_spend_last_7_days_688'),
        max(col('lifestyle_kids_category_spend_last_3_months_686')).alias(
            'lifestyle_kids_category_spend_last_3_months_686'),
        max(col('lifestyle_kids_category_spend_last_1_month_687')).alias(
            'lifestyle_kids_category_spend_last_1_month_687'),
        max(col('lifestyle_furniture_category_spend_last_7_days_676')).alias(
            'lifestyle_furniture_category_spend_last_7_days_676'),
        max(col('lifestyle_furniture_category_spend_last_3_months_674')).alias(
            'lifestyle_furniture_category_spend_last_3_months_674'),
        max(col('lifestyle_furniture_category_spend_last_1_month_675')).alias(
            'lifestyle_furniture_category_spend_last_1_month_675'),
        max(col('lifestyle_decor_category_spend_last_7_days_682')).alias(
            'lifestyle_decor_category_spend_last_7_days_682'),
        max(col('lifestyle_decor_category_spend_last_3_months_680')).alias(
            'lifestyle_decor_category_spend_last_3_months_680'),
        max(col('lifestyle_decor_category_spend_last_1_month_681')).alias(
            'lifestyle_decor_category_spend_last_1_month_681'),
        max(col('lifestyle_avg_spend_last_6_months_389')).alias('lifestyle_avg_spend_last_6_months_389'),
        max(col('lifestyle_avg_spend_last_3_months_390')).alias('lifestyle_avg_spend_last_3_months_390'),
        max(col('lifestyle_avg_spend_last_2_months_391')).alias('lifestyle_avg_spend_last_2_months_391'),
        max(col('lifestyle_avg_spend_last_1_month_392')).alias('lifestyle_avg_spend_last_1_month_392'),
        max(col('lec_total_spend_for_12_months_271')).alias('lec_total_spend_for_12_months_271'),
        max(col('lec_mp_avg_txn_value_for_12_months_289')).alias('lec_mp_avg_txn_value_for_12_months_289'),
        max(col('isd_code')).alias('isd_code'),
        max(col('coupon_generated')).alias('ind_coupon_generated'),
        max(col('cc_mall_code')).alias('ide_cc_mall_code'),
        max(col('gulf_news')).alias('gulf_news'),
        max(col('fab_cobrand_product')).alias('fab_cobrand_product'),
        max(col('des_summer_activity')).alias('des_summer_activity'),
        max(col('des_nationality')).alias('des_nationality'),
        max(col('des_membership_stage')).alias('des_membership_stage'),
        max(col('des_language_customer')).alias('des_language_customer'),
        max(col('des_language')).alias('des_language'),
        max(col('dat_registration_share')).alias('dat_registration_share'),
        max(col('dat_last_redemption')).alias('dat_last_redemption'),
        max(col('dat_last_login')).alias('dat_last_login'),
        max(col('dat_last_activity')).alias('dat_last_activity'),
        max(col('dat_last_accrual')).alias('dat_last_accrual'),
        max(col('dat_first_login_share')).alias('dat_first_login_share'),
        max(col('dat_first_login')).alias('dat_first_login'),
        max(col('dat_enrollment')).alias('dat_enrollment'),
        # max(col('dat_birth')).alias('dat_birth'),
        # max(col('cod_sor_counterparty')).alias('cod_sor_counterparty'),
        max(col('cde_mal')).alias('cde_mal'),
        max(col('cde_country_of_residence')).alias('cde_country_of_residence'),
        max(col('app_id')).alias('app_id')
    )

    # conf_braze = conf_braze.drop("bu").withColumnRenamed('coupon_generated', 'ind_coupon_generated').withColumnRenamed('cc_mall_code', 'ide_cc_mall_code')  # TODO should be fixced in ca merge job and share lib

    rfm = spark.read.csv('s3://cvm-prod-conformed-dd6241f/features/rfm/output/rfm.bu.share.proposition/', header=True)
    clv = spark.read.csv('s3://cvm-prod-conformed-dd6241f/features/clv/output/clv.bu.share/', header=True)
    kpi = DeltaTable.forPath(spark, get_s3_path(ProfileCalculatedKpi, lake_descriptor)).toDF()

    sponsor_dict = DeltaTable.forPath(spark,  get_s3_path(LoyaltySponsor, lake_descriptor)).toDF()
    prod = DeltaTable.forPath(spark,  get_s3_path(Products, lake_descriptor)).toDF()

    prod04_dict = prod.groupBy('idi_proposition_level04', 'nam_proposition_level04').count()
    prod04_dict.show()

    prod03_dict = prod.groupBy('idi_proposition_level03', 'nam_proposition_level03').count()
    prod03_dict.show()

    prod00_dict = prod.groupBy('idi_proposition', 'nam_proposition').count()
    prod00_dict.show()

    pao = DeltaTable.forPath(spark,  get_s3_path(ProfileAlgorithmsOutput, lake_descriptor)).toDF()

    result = run(conf_demographics, conf_id_map, conf_braze_max, rfm, clv, kpi, pao, sponsor_dict, prod04_dict, prod03_dict, prod00_dict)
    result.printSchema()

    creat_delta_table_if_not_exists(spark, CustomerProfile, lake_descriptor)

    profiles_s3_path = get_s3_path(CustomerProfile, lake_descriptor)

    # profiles_s3_path = 's3://cvm-prod-conformed-dd6241f/customer_profile_test_2023_07_24'

    result.write.format('delta').mode('overwrite').save(profiles_s3_path)
