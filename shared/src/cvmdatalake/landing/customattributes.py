from enum import Enum

from cvmdatalake import ColumnSpec, TableSpec


class CustomAttributes(TableSpec):

    @classmethod
    def table_description(cls):
        return "Custom Attributes from Braze"

    partition_0 = ColumnSpec(
        is_partition=True,
        data_type='string',
        description='Business Unit'
    )

    ca_138_smbu_total_spend_fashion_for_12_months = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_139_smbu_total_spend_electronics_for_12_months = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_140_smbu_total_spend_luxury_for_12_months = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_141_smbu_total_spend_fnb_for_12_months = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_142_smbu_total_spend_luxuryfashion_for_12_months = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_143_smbu_total_spend_furniture_household_for_12_months = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_144_smbu_total_spend_sports_for_12_months = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_148_smbu_avg_spend_fashion_for_12_months = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_149_smbu_avg_spend_electronics_for_12_months = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_150_smbu_avg_spend_luxury_for_12_months = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_151_smbu_avg_spend_fnb_for_12_months = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_152_smbu_avg_spend_luxuryfashion_for_12_months = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_153_smbu_avg_spend_furniture_household_for_12_months = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_154_smbu_avg_spend_sports_for_12_months = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_159_vox_avg_txn_value_for_12_months = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_164_vox_total_spend_for_12_months = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_169_vox_arabic_movie_count = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_170_vox_hindi_movie_count = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_172_vox_childrens_movie_count = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_175_vox_cinema_total_spend_last_12_months = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_176_vox_cinema_total_spend_last_6_months = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_177_vox_cinema_total_spend_last_3_months = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_180_maf_cinema_total_txn_last_12_months = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_181_maf_cinema_total_txn_last_6_months = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_182_maf_cinema_total_txn_last_3_months = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_191_vox_total_number_transactions = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_199_vox_family_movie_total_revenue = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_1_crf_total_spend_last_12_months = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_20230331_expiring_points = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_20230331_exp_points_worth_aed = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_2023q2_target_control = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_2023q2_target_control_filter = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_20_crf_last_transaction_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_214_vox_family_movie_total_txn = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_223_vox_scifi_movie_total_txn = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_231_vox_arabic_movie_total_txn = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_236_vox_kids_movie_total_txn = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_237_vox_preferred_mall = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_23_crf_preferred_store = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_244_vox_most_recent_movie_watched = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_245_vox_number_movies_last_1_month = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_246_lne_avg_txn_for_12_months = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_247_lne_no_txn_for_12_months = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_24_crf_preferred_city = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_250_lne_total_spend_for_12_months = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_258_lne_total_transactions_last_6_months = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_259_lne_total_trransactions_last_3_months = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_267_lne_avg_txn_for_12_months = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_26_crf_lifecycle_segment = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_271_lec_total_spend_for_12_months = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_289_lec_mp_avg_txn_value_for_12_months = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_301_ski_total_txn_last_6_months = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_302_ski_total_txn_last_3_months = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_30_crf_myclub_registration_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_32_crf_member_status = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_35_crf_member_shopper_group_segment = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_35_crf_member_shopper_group_segment_country = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_373_lifestyle_avg_txn_value_1_year = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_374_lifestyle_freq_of_txn_1_year = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_375_lifestyle_no_of_days_of_visit_1_year = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_376_lifestyle_last_trans_date = ColumnSpec(
        data_type='timestamp',
        description=''
    )

    ca_377_lifestyle_total_spend_1_year = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_378_lifestyle_total_spend_3_months = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_379_lifestyle_total_spend_6_months = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_380_lifestyle_total_spend_lifecycle = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_381_lifestyle_transaction_last_180_days = ColumnSpec(
        data_type='boolean',
        description=''
    )

    ca_382_lifestyle_months_dormant = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_383_lifestyle_total_spend_2_months = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_384_lifestyle_total_spend_1_month = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_385_lifestyle_total_txn_last_last_6_months = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_386_lifestyle_total_txn_last_3_months = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_387_lifestyle_total_txn_last_2_months = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_388_lifestyle_total_txn_last_1_month = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_389_lifestyle_avg_spend_last_6_months = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_38_crf_coupon_redeemed = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_390_lifestyle_avg_spend_last_3_months = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_391_lifestyle_avg_spend_last_2_months = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_392_lifestyle_avg_spend_last_1_month = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_40_crf_last_instore_txn = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_42_crf_supermarket_last_txn = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_43_crf_hypermarket_last_txn = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_44_crf_moemirates_last_txn = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_45_crf_ccderia_last_txn = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_46_crf_burjuman_last_txn = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_47_crf_ibnbattuta_last_txn = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_48_crf_ccsharjah_last_txn = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_49_crf_abudhabi_airportroad_last_txn = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_50_crf_ccfujairah_last_txn = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_51_crf_rakmanar_last_txn = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_52_crf_total_txn_instore_last_3_months = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_572_smbu_mall_aquisition = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_573_smbu_mall_visit_frequency_last_12_months = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_574_smbu_mall_visit_frequency_last_6_months = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_575_share_active_30_days = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_576_share_active_60_days = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_577_share_active_90_days = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_578_share_active_180_days = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_580_lifestyle_furniture_category_spend_last_1_year = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_581_lifestyle_decor_category_spend_last_1_year = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_582_lifestyle_kids_category_spend_last_1_year = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_668_crf_first_transaction_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_674_lifestyle_furniture_category_spend_last_3_months = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_675_lifestyle_furniture_category_spend_last_1_month = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_676_lifestyle_furniture_category_spend_last_7_days = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_67_crf_consumergood_instore_spend_last_3_months = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_680_lifestyle_decor_category_spend_last_3_months = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_681_lifestyle_decor_category_spend_last_1_month = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_682_lifestyle_decor_category_spend_last_7_days = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_686_lifestyle_kids_category_spend_last_3_months = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_687_lifestyle_kids_category_spend_last_1_month = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_688_lifestyle_kids_category_spend_last_7_days = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_68_crf_consumergood_instore_spend_last_3_months = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_690_lifestyle_accessories_category_spend_last_3_months = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_691_lifestyle_accessories_category_spend_last_1_month = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_692_lifestyle_accessories_category_spend_last_7_days = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_697_lifestyle_jewelry_category_spend_last_3_months = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_698_lifestyle_jewelry_category_spend_last_1_month = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_699_lifestyle_jewelry_category_spend_last_7_days = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_69_crf_heavyhousehold_instore_spend_last_3_months = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_704_lifestyle_fitness_clothing_category_spend_last_3_months = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_705_lifestyle_fitness_clothing_category_spend_last_1_month = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_706_lifestyle_fitness_clothing_category_spend_last_7_days = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_70_crf_lighthousehold_instore_spend_last_3_months = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_711_lifestyle_fitness_equipment_category_spend_last_3_months = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_712_lifestyle_fitness_equipment_category_spend_last_1_month = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_713_lifestyle_fitness_equipment_category_spend_last_7_days = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_718_lifestyle_lego_duplo_category_spend_last_3_months = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_719_lifestyle_lego_duplo_category_spend_last_1_month = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_71_crf_textiles_instore_spend_last_3_months = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_720_lifestyle_lego_duplo_category_spend_last_7_days = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_725_lifestyle_run_category_spend_last_3_months = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_726_lifestyle_run_category_spend_last_1_month = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_727_lifestyle_run_category_spend_last_7_days = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_72_crf_consumergood_no_txn_instore_last_3_months = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_732_lifestyle_yoga_category_spend_last_3_months = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_733_lifestyle_yoga_category_spend_last_1_month = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_734_lifestyle_yoga_category_spend_last_7_days = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_739_lifestyle_lego_kids_category_spend_last_3_months = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_73_crf_freshfood_no_txn_instore_last_3_months = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_740_lifestyle_lego_kids_category_spend_last_1_month = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_741_lifestyle_lego_kids_category_spend_last_7_days = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_746_lifestyle_lego_adults_category_spend_last_3_months = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_747_lifestyle_lego_adults_category_spend_last_1_month = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_748_lifestyle_lego_adults_category_spend_last_7_days = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_74_crf_heavyhousehold_no_txn_instore_last_3_months = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_75_crf_lighthousehold_no_txn_instore_last_3_months = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_76_crf_textiles_no_txn_instore_last_3_months = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_878_lifestyle_footwear_category_spend_last_3_months = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_879_lifestyle_footwear_category_spend_last_1_month = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_880_lifestyle_footwear_category_spend_last_7_days = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_881_lifestyle_transaction_frequency_moe_last_3_months = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_882_lifestyle_transaction_frequency_moe_last_1_month = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_883_lifestyle_transaction_frequency_ccmi_last_3_months = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_884_lifestyle_transaction_frequency_ccmi_last_1_month = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_885_offers_activated_3m = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_886_offers_activated_6m = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_887_offers_activated_lifetime = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_888_receipt_scanning_failed_lifetime = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_889_receipt_scanning_failed_6m = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_890_receipt_scanning_failed_3m = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_891_receipt_scanning_failed_1m = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_892_receipt_scanning_failed_c4_lifetime = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_893_receipt_scanning_failed_c4_6m = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_894_receipt_scanning_failed_c4_3m = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_895_receipt_scanning_failed_c4_1m = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_896_receipt_scanning_failed_c4_7d = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_897_receipt_scanning_succeeded_lifetime = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_898_receipt_scanning_succeeded_6m = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_899_receipt_scanning_succeeded_3m = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_900_receipt_scanning_succeeded_1m = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_901_share_pay_active_3m = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_902_share_pay_active_6m = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_903_share_pay_active_lifetime = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_904_date_of_first_earn = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_905_date_of_last_earn = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_906_date_of_first_redemption = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_907_date_of_last_redemption = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_908_count_of_sharepay_earn_lifetime = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_909_count_of_sharepay_earn_7d = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_910_count_of_share_id_earn_lifetime = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_911_count_of_share_id_earn_7d = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_912_lifestyle_total_luxury_spend_last_3_months = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_913_lifestyle_total_luxury_spend_last_1_month = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_914_lifestyle_total_luxury_spend_last_7_days = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_915_lifestyle_holiday_category_spend_last_3_months = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_916_lifestyle_holiday_category_spend_last_1_month = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_917_lifestyle_holiday_category_spend_last_7_days = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_918_lifestyle_clothing_category_spend_last_3_months = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_919_lifestyle_clothing_category_spend_last_1_month = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_920_lifestyle_essential_fashion_category_spend_last_3_months = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_921_lifestyle_essential_fashion_category_spend_last_1_month = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_926_lifestyle_visit_frequency_moe_last_3_months = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_927_lifestyle_visit_frequency_moe_last_1_month = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_928_lifestyle_visit_frequency_ccmi_last_3_months = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_929_lifestyle_visit_frequency_ccmi_last_1_month = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_938_smbu_total_spend_luxury_for_1_month = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_939_smbu_total_spend_luxury_for_3_months = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_940_smbu_total_spend_luxury_for_6_months = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_946_lifestyle_luxury_spend_but_not_in_that_last_3_months = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_947_lifestyle_luxury_spend_but_not_in_that_last_1_month = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_948_lifestyle_gift_card_trx_count_last_3_months = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_949_lifestyle_share_point_redemption_count_last_3_months = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_950_lifestyle_promocode_redemption_count_last_3_months = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_951_lifestyle_promocode_redemption_count_last_1_month = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_952_lifestyle_share_point_redemption_count_last_1_month = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_953_lifestyle_kitchen_category_spend_last_3_months = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_954_lifestyle_kitchen_category_spend_last_1_month = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_955_lifestyle_kitchen_category_spend_last_7_days = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_956_lifestyle_luxury_spend_but_not_in_als_last_3_months = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_957_lifestyle_luxury_spend_but_not_in_als_last_1_month = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_958_lifestyle_furniture_spend_but_not_in_cb_last_3_months = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_959_lifestyle_furniture_spend_but_not_in_cb_last_1_month = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_960_lifestyle_furniture_spend_but_not_in_cb2_last_3_months = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_961_lifestyle_furniture_spend_but_not_in_cb2_last_1_month = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_abercrombiefitch_email_subscribe = ColumnSpec(
        data_type='boolean',
        description=''
    )

    ca_abercrombiefitch_unsubscribe_date = ColumnSpec(
        data_type='timestamp',
        description=''
    )

    ca_abercrombiefitch_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_allsaints_email_subscribe = ColumnSpec(
        data_type='boolean',
        description=''
    )

    ca_allsaints_registration_date = ColumnSpec(
        data_type='timestamp',
        description=''
    )

    ca_allsaints_unsubscribe_date = ColumnSpec(
        data_type='timestamp',
        description=''
    )

    ca_allsaints_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_apps = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_app_country = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_app_id = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_app_language = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_april_tp_offer_link = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_april_tp_offer_link_update = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_april_tp_offer_link_update_j2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_april_tp_spend_threshold = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_april_tp_spend_threshold_j2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_area_code = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_automated_preferredstores_trigger = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_automated_test_low_extra_low_trigger = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_a_like = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_babies_products = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_big_fish_share_of_wallet = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_bio_products = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_bottom_banner_image_url = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_bought_xy_products = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_brand = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_braze_external_id = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_braze_id = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_c4_bahrain_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_c4_bahrain_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_c4_bahrain_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_c4_egypt_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_c4_egypt_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_c4_egypt_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_c4_georgia_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_c4_georgia_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_c4_georgia_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_c4_grg_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_c4_grg_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_c4_grg_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_c4_jordan_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_c4_jordan_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_c4_jordan_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_c4_kenya_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_c4_kenya_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_c4_kenya_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_c4_ksa_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_c4_ksa_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_c4_ksa_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_c4_kuwait_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_c4_kuwait_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_c4_kuwait_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_c4_lebanon_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_c4_lebanon_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_c4_lebanon_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_c4_oman_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_c4_oman_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_c4_oman_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_c4_pakistan_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_c4_pakistan_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_c4_pakistan_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_c4_push_marketing_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_c4_push_myorders_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_c4_qatar_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_c4_qatar_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_c4_qatar_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_c4_uae_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_c4_uae_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_c4_uae_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_c4_uzbekistan_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_c4_uzbekistan_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_c4_uzbekistan_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_campaign_test_mail = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_carrefour_registration_date = ColumnSpec(
        data_type='timestamp',
        description=''
    )

    ca_cart_count = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_category_level2_typ_reco_cgd_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_category_products = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_cb2_email_subscribe = ColumnSpec(
        data_type='boolean',
        description=''
    )

    ca_cb2_registration_date = ColumnSpec(
        data_type='timestamp',
        description=''
    )

    ca_cb2_unsubscribe_date = ColumnSpec(
        data_type='timestamp',
        description=''
    )

    ca_cb2_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_cbu_registration_date = ColumnSpec(
        data_type='timestamp',
        description=''
    )

    ca_cb_email_subscribe = ColumnSpec(
        data_type='boolean',
        description=''
    )

    ca_cb_registration_date = ColumnSpec(
        data_type='timestamp',
        description=''
    )

    ca_ccmi_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_ccmi_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_ccmi_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_cc_ajman_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_cc_ajman_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_cc_ajman_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_cc_alexandria_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_cc_alexandria_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_cc_alexandria_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_cc_almaza_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_cc_almaza_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_cc_almaza_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_cc_alshindagha_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_cc_alshindagha_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_cc_alshindagha_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_cc_alzahia_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_cc_alzahia_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_cc_alzahia_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_cc_bahrain_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_cc_bahrain_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_cc_bahrain_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_cc_deira_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_cc_deira_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_cc_deira_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_cc_fujairah_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_cc_fujairah_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_cc_fujairah_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_cc_maadi_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_cc_maadi_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_cc_maadi_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_cc_mall_code = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_cc_meaisem_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_cc_meaisem_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_cc_meaisem_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_cc_mirdiff_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_cc_mirdiff_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_cc_mirdiff_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_cc_muscat_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_cc_muscat_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_cc_muscat_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_cc_qurum_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_cc_qurum_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_cc_qurum_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_cc_sharjah_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_cc_sharjah_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_cc_sharjah_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_cc_suhar_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_cc_suhar_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_cc_suhar_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_churn_prop = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_cod_sor_counterparty = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_cod_sor_gr = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_country = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_country_of_residence = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_coupon_generated = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_coupon_1 = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_coupon_2 = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_coupon_code = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_cratebarrelzzz_email_subscribe = ColumnSpec(
        data_type='boolean',
        description=''
    )

    ca_cratebarrelzzz_unsubscribe_date = ColumnSpec(
        data_type='timestamp',
        description=''
    )

    ca_cratebarrel_email_subscribe = ColumnSpec(
        data_type='boolean',
        description=''
    )

    ca_cratebarrel_registration_date = ColumnSpec(
        data_type='timestamp',
        description=''
    )

    ca_cratebarrel_unsubscribe_date = ColumnSpec(
        data_type='timestamp',
        description=''
    )

    ca_cratebarrel_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_created_at = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_customer_email = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_customer_language = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_customer_title = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_custom_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_custom_events = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_custom_property = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_custom_property1 = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_custom_property2 = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_custom_proprty1 = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_cvm_audience = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_date_of_birth = ColumnSpec(
        data_type='timestamp',
        description=''
    )

    ca_devices = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_discount = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_discount_alert_products = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_dla_audience_membership = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_dla_file_upload_membership = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_dob = ColumnSpec(
        data_type='timestamp',
        description=''
    )

    ca_dreamscape_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_dreamscape_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_dreamscape_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_email = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_email_id = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_email_items = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_email_verified = ColumnSpec(
        data_type='boolean',
        description=''
    )

    ca_email_verified_date = ColumnSpec(
        data_type='timestamp',
        description=''
    )

    ca_enrollment_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_enrollment_sponsor = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_enrolment_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_eow_5xreminder_trigger = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_eow_butcherypoultryonlyoverall_trigger = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_eow_butcherypoultryonlypromo_trigger = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_eow_dairy_trigger = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_eow_dphallproducts_trigger = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_eow_electronicsallproducts_trigger = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_eow_fballproducts_trigger = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_eow_fbmasspromo_underreached = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_eow_festivepreorder_trigger = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_eow_fruitsveggiesallproducts_trigger = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_eow_fruitsveggies_trigger = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_eow_fruitsvegproductsonlypromo_trigger = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_eow_grocerypromo_trigger = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_eow_grocery_trigger = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_eow_household_trigger = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_eow_nuts_trigger = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_eow_outdoor_trigger = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_eow_recononfood_underreached = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_eow_reco_butcherypoultryonlyoverall_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_eow_reco_butcherypoultryonlypromo_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_eow_reco_dairy_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_eow_reco_dphallproducts_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_eow_reco_electronicsallproducts_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_eow_reco_fballproducts_beverage = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_eow_reco_fballproducts_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_eow_reco_fballproducts_grocery = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_eow_reco_fbmasspromo = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_eow_reco_fbmasspromo2_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_eow_reco_fbmasspromo_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_eow_reco_fruitsveggiesallproducts_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_eow_reco_fruitsveggies_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_eow_reco_fruitsvegproductsonlypromo_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_eow_reco_grocerypromo_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_eow_reco_grocery_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_eow_reco_household_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_eow_reco_nuts_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_eow_reco_outdoor_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_eow_reco_outdoor_outdoor_grill = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_eow_reco_outdoor_outdoor_others = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_eow_reco_recononfood = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_eow_reco_toys_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_eow_reco_ultrafreshallproducts_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_eow_share5x_trigger = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_eow_test_attribute = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_eow_test_attribute_2 = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_eow_test_attribute_3 = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_eow_toys_trigger = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_eow_ultrafreshallproducts_trigger = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_external_id = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_fab_cc_issue_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_fab_cobrand_product = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_fab_customer_id = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_fashion4less_email_subscribe = ColumnSpec(
        data_type='boolean',
        description=''
    )

    ca_fashion4less_unsubscribe_date = ColumnSpec(
        data_type='timestamp',
        description=''
    )

    ca_fashion4less_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_favfruit = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_favorite_category = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_feed_me_labneh_ids = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_firstname_gcr = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_first_fab_earn_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_first_login_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_first_name = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_first_share_login_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_flag_abd_store = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_flag_dub_store = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_fnb_als = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_fnb_als_topsellers = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_fnb_cross_sell = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_fnb_cross_sell_als = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_fnb_frequency_not_promo_products = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_fnb_frequency_products = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_fnb_topsellers = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_gender = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_globalsnow_booking_visit_date = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_globalsnow_language = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_globalsnow_website_language = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_global_snow_next_visit_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_gulf_news = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_happiness_lab_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_happiness_lab_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_hollister_email_subscribe = ColumnSpec(
        data_type='boolean',
        description=''
    )

    ca_hollister_unsubscribe_date = ColumnSpec(
        data_type='timestamp',
        description=''
    )

    ca_hollister_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_home_city = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_hv_end_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_hv_enrolment_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_hv_optin_status = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_hv_segment = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_hv_tier = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_iamai_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_iamai_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_iamai_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_ide_email = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_idfa_setting = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_idi_counterparty_gr = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_ifly_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_ifly_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_ifly_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_isd_code = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_item_order_value = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_ken_18 = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_language = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_lastname_gcr = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_last_accrual_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_last_activity_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_last_coordinates = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_last_login_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_last_name = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_last_redemption_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_last_viewed_page = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_latitude = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_lego_email_subscribe = ColumnSpec(
        data_type='boolean',
        description=''
    )

    ca_lego_registration_date = ColumnSpec(
        data_type='timestamp',
        description=''
    )

    ca_lego_unsubscribe_date = ColumnSpec(
        data_type='timestamp',
        description=''
    )

    ca_lego_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_lifecycle = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_littleexplorers_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_littleexplorers_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_littleexplorers_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_loginfrom = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_login_status = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_longitude = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_lululemon_email_subscribe = ColumnSpec(
        data_type='boolean',
        description=''
    )

    ca_lululemon_registration_date = ColumnSpec(
        data_type='timestamp',
        description=''
    )

    ca_lululemon_unsubscribe_date = ColumnSpec(
        data_type='timestamp',
        description=''
    )

    ca_lululemon_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_mafsb_campaigns = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_mafsb_churned = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_mafsb_groups = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_magicplanet_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_magicplanet_registration_date = ColumnSpec(
        data_type='timestamp',
        description=''
    )

    ca_magicplanet_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_magicplanet_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_maisonsdumonde_email_subscribe = ColumnSpec(
        data_type='boolean',
        description=''
    )

    ca_mall_code = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_matajer_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_matajer_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_meal_ids = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_membership_stage = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_member_email = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_moegypt_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_moegypt_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_moegypt_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_moemirates_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_moemirates_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_moemirates_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_moe_language = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_moe_registration_date = ColumnSpec(
        data_type='timestamp',
        description=''
    )

    ca_mooman_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_mooman_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_mooman_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_mycc_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_mycc_masdar_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_mycc_masdar_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_mycc_masdar_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_mycc_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_mycc_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_nationality = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_new_cust_flag = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_non_food_cross_sell = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_non_food_cross_sell_als = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_non_food_cross_sell_mba = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_non_food_cross_sell_promo = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_non_food_frequency = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_non_food_topsellers = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_non_opener_email = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_non_opener_push = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_nps_addtext = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_nps_consignment_code = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_nps_delivery_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_nps_item_count = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_nps_order_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_nps_order_number = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_nps_product_code = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_nps_product_url = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_nps_rating = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_nps_reasons = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_offer_link = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_offline_first_maf_contact = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_order_count = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_order_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_order_id = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_organic_product_list = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_otp_verified = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_persona = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_personalized_push_data = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_phone = ColumnSpec(
        data_type='double',
        description=''
    )

    ca_points_balance = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_points_won_spin_wheel = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_poltrona_frau_email_subscribe = ColumnSpec(
        data_type='boolean',
        description=''
    )

    ca_poltrona_frau_unsubscribe_date = ColumnSpec(
        data_type='timestamp',
        description=''
    )

    ca_poltrona_frau_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_preferred_store_name = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_previous_last_coordinates = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_productids_cross_sell = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_productids_default = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_productids_frequency = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_productids_up_sell = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_product_1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_product_2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_product_id = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_product_ids = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_product_list = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_product_list_1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_product_list_2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_product_list_3 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_product_list_33 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_product_list_t1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_product_list_t1_new = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_product_t12_organic = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_promo_code = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_promo_code_ar = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_promo_code_en = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_promo_for_churn_products = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_purchases = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_purchase_channel = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_purchase_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_push_items = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_push_token = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_pv_product_list = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_random_bucket = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_recent_categories = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_recommendations = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_recommendations_food = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_recommendations_nonfood = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_recommendations_non_food = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_recommendation_scores_grocery_2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_recommended_category = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_reco_als = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_reco_topsellers = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_registeredfrom = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_registered_from = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_registration_date = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_relationship_family_status = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_reward_points = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sales_boost_language = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_saved_card = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_sb_big_fish = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_churned_categories = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_3mpost_it = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_bakery_pastry = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_beverage = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_butchery = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_crayola = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_dairy_products_ss = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_elmers = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_faber_castel = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_fruits_vegetables = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_funbo = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_grocery = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_100percent20cus_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_20discount_04_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_abdudhabi_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_antichurn_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_april_1st_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_april_8_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_avataar_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_baby_1117_family = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_beautyt4_section = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_beauty_02_section = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_beauty_1115_section = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_beauty_1206_section = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_beauty_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_beauty_bigfish_section = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_beauty_new_section = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_beauty_section = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_bigdiscount_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_blending_family = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_cat_food_family = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_churned_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_color_india_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_couponfujairah_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_cutomersexpiredcouponall3_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_cutomersexpiredcouponnew_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_cutomersexpiredcoupon_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_diwali_big_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_dog_food_family = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_dsf10_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_dubaistore_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_dubaistore_basket_new = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_electronics_1118_section = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_expiry_week_4thfeb_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_expiry_week_5thfeb_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_feb_12_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_feb_18_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_feb_19_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_feb_25_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_feb_26_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_jan_02_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_jan_03_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_jan_04_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_location2410_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_location2410_section = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_locations_0311_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_locations_1110_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_locations_1117_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_locations_1121_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_locations_1124_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_locations_1201_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_locations_1205_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_locations_1215_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_locations_1215_new_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_locations_1222_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_locations_3110_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_locations_amsaf_03_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_locations_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_locations_test_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_location_1207_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_location_2409_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_location_2508_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_location_2520_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_location_all_low_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_location_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_location_big_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_lowetxralow_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_mar_4_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_mar_4_mar_11_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_multichannel_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_new_0921 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_organic_sub_family = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_physical = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_preferredstores_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_preferredstore_16_april_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_preferredstore_1st_april_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_preferredstore_1stapril_churned_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_preferredstore_26march_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_preferredstore_2ndweek_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_preferredstore_35_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_preferredstore_36_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_preferredstore_feb18_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_preferredstore_section = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_promosensitive1104_section = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_promosensitive1104_section_new = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_promosensitive1114_section = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_promosensitive_family = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_promosensitive_section = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_s22_t2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_sharjah_02_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_sow_10_20_40_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_t15 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_t15_coupon_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_t16_coupon_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_t16_coupon_section = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_t2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_t2_1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_t3 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_t3_big = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_t6 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_testingcoupon_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_list_underreached_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_sharpie = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_coupons_stabilo = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_frequency = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sb_lifecycle = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_sb_recent_categories = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_segment = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_share_app_country = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_share_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_share_enrollment = ColumnSpec(
        data_type='boolean',
        description=''
    )

    ca_share_id = ColumnSpec(
        data_type='bigint',
        description=''
    )

    ca_share_member_id = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_share_points_braze = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_share_points_braze_2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_share_pseudo_id = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_share_registration_date = ColumnSpec(
        data_type='timestamp',
        description=''
    )

    ca_share_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_share_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_sharjah_registration_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_shiseido_email_subscribe = ColumnSpec(
        data_type='boolean',
        description=''
    )

    ca_shiseido_unsubscribe_date = ColumnSpec(
        data_type='timestamp',
        description=''
    )

    ca_shiseido_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_shishiedo_registration_date = ColumnSpec(
        data_type='timestamp',
        description=''
    )

    ca_skidubai_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_skidubai_registration_date = ColumnSpec(
        data_type='timestamp',
        description=''
    )

    ca_skidubai_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_skidubai_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_skyfii_last_activity_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_skyfii_registered_from = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_skyfii_registration_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_smiles_activity_count = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_smiles_last_activity_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_snowoman_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_snowoman_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_snowoman_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_sprint_23_reco_test_18 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_23_reco_test_19_female = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_23_reco_test_19_male = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_23_reco_test_20_female = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_23_reco_test_20_male = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_23_reco_test_21_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_23_reco_test_22_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_23_reco_test_24_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_23_reco_test_25_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_23_reco_test_26_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_23_reco_test_28_new = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_23_reco_test_28_new2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_24_reco_test_10_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_24_reco_test_11_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_24_reco_test_12_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_24_reco_test_16_arab = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_24_reco_test_16_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_24_reco_test_16_indian = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_24_reco_test_16_indians = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_24_reco_test_16_philipino = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_24_reco_test_16_philipinos = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_24_reco_test_16_western = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_24_reco_test_16_westerners = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_24_reco_test_17_female = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_24_reco_test_17_male = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_24_reco_test_19_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_24_reco_test_1_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_24_reco_test_20_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_24_reco_test_21_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_24_reco_test_23_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_24_reco_test_3_female = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_24_reco_test_3_male = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_24_reco_test_4_female = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_24_reco_test_4_male = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_24_reco_test_7_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_24_reco_test_8_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_25_reco_test_10a_acc = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_25_reco_test_10a_food = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_25_reco_test_10b_acc = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_25_reco_test_10b_acc_new = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_25_reco_test_10b_food = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_25_reco_test_10b_food_new = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_25_reco_test_11_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_25_reco_test_11_default_new = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_25_reco_test_11_food_prep = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_25_reco_test_11_toast_grill = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_25_reco_test_12_food = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_25_reco_test_12_outdoor_fur = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_25_reco_test_12_outdoor_grill = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_25_reco_test_12_outdoor_others = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_25_reco_test_13_food = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_25_reco_test_13_outdoor_fur = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_25_reco_test_13_outdoor_grill = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_25_reco_test_13_outdoor_others = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_25_reco_test_1601_lights_new_top = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_25_reco_test_1601_sweets_new_top = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_25_reco_test_16_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_25_reco_test_1701_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_25_reco_test_17_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_25_reco_test_18_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_25_reco_test_20_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_25_reco_test_20_default_new = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_25_reco_test_2101_acc = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_25_reco_test_2101_acc_new = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_25_reco_test_2101_food = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_25_reco_test_2101_food_new = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_25_reco_test_2102_acc = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_25_reco_test_2102_food = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_25_reco_test_24_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_25_reco_test_25_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_25_reco_test_26_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_25_reco_test_3_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_25_reco_test_3_default_new = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_25_reco_test_4_female = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_25_reco_test_4_female_new = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_25_reco_test_4_male = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_25_reco_test_4_male_new = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_25_reco_test_6_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_25_reco_test_7_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_25_reco_test_9b_acc = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_25_reco_test_9b_acc_new = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_25_reco_test_9b_food = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_25_reco_test_9b_food_new = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_25_reco_test_9_acc = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_25_reco_test_9_acc_new = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_25_reco_test_9_food = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_25_reco_test_9_food_new = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_26_reco_test_1101_snacks = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_26_reco_test_11_snacks = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_26_reco_test_1701_acc = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_26_reco_test_1701_food = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_26_reco_test_17_acc = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_26_reco_test_17_food = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_26_reco_test_1901_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_26_reco_test_19_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_26_reco_test_1_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_26_reco_test_20_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_26_reco_test_24_projectors = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_26_reco_test_24_speakers = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_26_reco_test_24_tv = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_26_reco_test_25_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_26_reco_test_26_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_26_reco_test_2_female = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_26_reco_test_2_male = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_26_reco_test_3_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_26_reco_test_401_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_26_reco_test_4_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_26_reco_test_5_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_26_reco_test_8_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_27_reco_test_11_acc = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_27_reco_test_11_adultcycle = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_27_reco_test_11_kidscycle = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_27_reco_test_23_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_27_reco_test_24_female = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_27_reco_test_24_male = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_27_reco_test_25_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_27_reco_test_25_default2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_27_reco_test_27_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_27_reco_test_28_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_27_reco_test_29_acc = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_27_reco_test_29_acc2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_27_reco_test_29_adultcycle2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_27_reco_test_29_adultcycle3 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_27_reco_test_30_acc = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_27_reco_test_30_acc_new = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_27_reco_test_30_food = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_27_reco_test_30_food_new = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_27_reco_test_31_acc = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_27_reco_test_31_food = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_27_reco_test_33_default_added = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_27_reco_test_3_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_27_reco_test_4_female = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_27_reco_test_4_female_new = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_27_reco_test_4_male = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_27_reco_test_4_male_new = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_27_reco_test_5_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_27_reco_test_6_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_27_reco_test_7_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_28_reco_test_10_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_28_reco_test_15_furniture = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_28_reco_test_15_grill = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_28_reco_test_1901_default2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_28_reco_test_1901_default_tree = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_28_reco_test_19_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_28_reco_test_19_default2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_28_reco_test_19_default3 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_28_reco_test_19_default4 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_28_reco_test_20_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_28_reco_test_20_default1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_28_reco_test_20_default_elec = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_28_reco_test_21_default1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_28_reco_test_21_default_elec_big = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_28_reco_test_22_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_28_reco_test_22_kitchen = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_28_reco_test_24_furniture = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_28_reco_test_26_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_28_reco_test_27_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_28_reco_test_3_female = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_28_reco_test_3_male = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_28_reco_test_4_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_28_reco_test_6_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_28_reco_test_7_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_28_reco_test_8_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_29_reco_test_10_proj = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_29_reco_test_10_speaker = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_29_reco_test_10_tv = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_29_reco_test_11_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_29_reco_test_16_default1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_29_reco_test_17_default1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_29_reco_test_3_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_29_reco_test_4_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_29_reco_test_4_default1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_29_reco_test_4_default2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_29_reco_test_4_default3 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_29_reco_test_4_default4 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_29_reco_test_5_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_29_reco_test_6_default2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_29_reco_test_9_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_29_reco_test_9_default1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_30_reco_test_02_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_30_reco_test_02_default1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_30_reco_test_02_delic = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_30_reco_test_07_hhh = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_30_reco_test_07_hhh1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_30_reco_test_07_lhh = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_30_reco_test_07_lhh1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_30_reco_test_12_acc2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_30_reco_test_12_food = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_30_reco_test_13_acc = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_30_reco_test_13_food = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_30_reco_test_1801_men_grooming = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_30_reco_test_1801_men_skincare = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_30_reco_test_18_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_30_reco_test_18_default1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_30_reco_test_18_female_hair = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_30_reco_test_18_female_skin = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_30_reco_test_19_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_30_reco_test_201_av = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_30_reco_test_201_kitchen = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_30_reco_test_20_av = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_30_reco_test_20_kitchen = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_30_reco_test_20_kitchen_group1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_30_reco_test_2201_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_30_reco_test_22_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_30_reco_test_5_default1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_30_reco_test_5_food_beverages = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_30_reco_test_701_hhh = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_30_reco_test_701_lhh = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_31_reco_test_12_oral_care = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_31_reco_test_1401_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_31_reco_test_14_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_31_reco_test_1602_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_31_reco_test_16_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_31_reco_test_1701_hhh = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_31_reco_test_1701_lhh = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_31_reco_test_17_hhh = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_31_reco_test_17_lhh = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_31_reco_test_18_default2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_31_reco_test_18_default3 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_31_reco_test_401_acc2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_31_reco_test_401_acc3 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_31_reco_test_401_products2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_31_reco_test_401_products_big = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_31_reco_test_401_products_big1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_31_reco_test_401_products_small = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_31_reco_test_401_products_small1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_31_reco_test_4_acc2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_31_reco_test_4_products2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_31_reco_test_4_products_big = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_31_reco_test_4_products_small = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_31_reco_test_5_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_31_reco_test_702_audio_speakers = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_31_reco_test_702_tvs = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_31_reco_test_7_audio_speakers = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_31_reco_test_7_audio_speakers_new1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_31_reco_test_7_tvs = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_31_reco_test_7_tvs_new = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_31_reco_test_8_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_32_reco_test_1001_section_1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_32_reco_test_1001_section_2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_32_reco_test_10_section1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_32_reco_test_10_section2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_32_reco_test_10_section__1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_32_reco_test_18_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_32_reco_test_18_garden_1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_32_reco_test_19_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_32_reco_test_19_default_new = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_32_reco_test_20_female = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_32_reco_test_20_male = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_32_reco_test_21_female = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_32_reco_test_21_male = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_32_reco_test_21_male_new = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_32_reco_test_24_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_32_reco_test_24_default_new = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_32_reco_test_25_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_32_reco_test_26_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_32_reco_test_6_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_32_reco_test_7_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_33_reco_test_04_female = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_33_reco_test_04_male = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_33_reco_test_08_ebars = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_33_reco_test_08_edrink = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_33_reco_test_11_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_33_reco_test_12_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_33_reco_test_12_default_new = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_33_reco_test_12_other_meat = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_33_reco_test_13_storage = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_33_reco_test_14_storage = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_33_reco_test_14_storage_1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_33_reco_test_18_defaults = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_33_reco_test_19_default_all = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_33_reco_test_19_default_all_1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_33_reco_test_20_default_all_30_1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_33_reco_test_24_default_camp = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_33_reco_test_24_default_camp_new = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_33_reco_test_24_default_new = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_33_reco_test_25_defaults = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_33_reco_test_25_defaults_new = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_33_reco_test_25_defaults_new3 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_33_reco_test_25_electronics = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_33_reco_test_25_recommendations = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_33_reco_test_25_regular = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_sprint_33_reco_test_25_thursday = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_33_reco_test_25_thursday_new = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_33_reco_test_27_chicken = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_33_reco_test_27_chicken_lovers = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_33_reco_test_28_chicken = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_33_reco_test_29_pizza = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_33_reco_test_30_valentine = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_sprint_33_reco_test_31_hsh = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_sprint_33_reco_test_6_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_33_reco_test_7_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_33_test_25_trigger = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_sprint_34_reco_test_11_defaults = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_34_reco_test_12_default1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_34_reco_test_12_defaults = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_34_reco_test_13_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_34_reco_test_14_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_34_reco_test_16_default_all_1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_34_reco_test_16_lebanese = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_34_reco_test_16_lebanese1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_34_reco_test_16_lebanese2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_34_reco_test_1901_dph_churned = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_34_reco_test_1901_dph_grocery = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_34_reco_test_19_defaults = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_34_reco_test_19_dph = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_34_reco_test_19_dph_2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_34_reco_test_19_grocery = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_34_reco_test_19_grocery_2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_34_reco_test_20_defaults = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_34_reco_test_20_dph = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_34_reco_test_20_dph1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_34_reco_test_20_dph_2_bf = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_34_reco_test_20_grocery = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_34_reco_test_20_grocery1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_34_reco_test_20_grocery_2_bf = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_34_reco_test_23_defaults = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_34_reco_test_23_veg = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_34_reco_test_24_defaults = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_34_reco_test_26_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_34_reco_test_27_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_34_reco_test_28_recommendations = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_34_reco_test_2_female = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_34_reco_test_2_male = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_34_reco_test_4_phili = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_34_reco_test_7_baby = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_34_reco_test_8_baby = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_34_test_10_coupons = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_35_reco_test_10_default2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_35_reco_test_19_defaults = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_35_reco_test_27_defaults = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_35_reco_test_29_coffee_1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_35_reco_test_29_defaults = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_35_reco_test_29_defaults_new = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_35_reco_test_2_egypt = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_35_reco_test_33_default2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_35_reco_test_34_default2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_35_reco_test_7_skincare = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_35_reco_test_7_skin_3 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_35_reco_test_7_skin_5 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_35_reco_test_7_skin_6 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_35_reco_test_7_skin_7 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_35_reco_test_9_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_35_reco_test_9_default2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_35_reco_test_9_org_1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_35_reco_test_9_org_2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_35_reco_test_9_org_3 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_36_reco_test_12_electronics = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_36_reco_test_12_fresh_veg = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_36_reco_test_13_electronics = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_36_reco_test_13_fresh_veg = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_36_reco_test_15_defaults = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_36_reco_test_20_grocery_1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_36_reco_test_20_grocery_2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_36_reco_test_20_home_1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_36_reco_test_20_personalcare_1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_36_reco_test_31_default2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_36_reco_test_31_default2_new = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_36_reco_test_32_default2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_36_reco_test_4_defaults = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_36_reco_test_4_defaults_1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_36_reco_test_5_defaults_1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_36_reco_test_8_baby_1_1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_36_reco_test_8_baby_1_2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_36_reco_test_8_baby_food = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_36_reco_test_8_baby_other = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_36_reco_test_9_defaults = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_36_reco_test_9_org_1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_36_reco_test_9_org_2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_37_reco_test_10_defaults = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_37_reco_test_18_defaults = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_37_reco_test_26_default2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_37_reco_test_27_default2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_37_reco_test_29_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_37_reco_test_9_defaults = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_38_reco_test_14_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_38_reco_test_19_default2temp = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_38_reco_test_20_default2temp = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_38_reco_test_8_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_38_reco_test_8_default2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_39_reco_test_10_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_39_reco_test_10_default_new = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_39_reco_test_12_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_39_reco_test_15_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_39_reco_test_16_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_39_reco_test_27_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_39_reco_test_27_default1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_39_reco_test_28_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_39_reco_test_28_default1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_39_reco_test_30_fruits = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_39_reco_test_30_fruits1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_39_reco_test_30_fruits2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_39_reco_test_30_vegetables = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_39_reco_test_30_vegetables1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_39_reco_test_30_vegetables2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_39_reco_test_31_fruits = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_39_reco_test_31_fruits1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_39_reco_test_31_fruits2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_39_reco_test_31_vegetables = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_39_reco_test_31_vegetables1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_39_reco_test_31_vegetables2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_39_reco_test_34_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_39_reco_test_3_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_39_reco_test_4_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_39_reco_test_8_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_39_reco_test_8_default_new = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_39_reco_test_9_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sprint_reco_recoattributes_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_storename_ar = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_storename_en = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_subject_line_one = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_subject_line_two = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_subscription_id = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_substitution_url = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_summer_activity = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_sw_completion_status = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_sw_grocery_last_bit_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_sw_grocery_status = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_sw_hotel_last_bit_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_sw_hotel_status = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_sw_lifestyle_last_bit_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_sw_lifestyle_status = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_sw_l_and_e_last_bit_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_sw_l_and_e_status = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_sw_mall_last_bit_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_sw_mall_status = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_sw_segment_code = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_template_final_name = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_test_attribute = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_that_email_subscribe = ColumnSpec(
        data_type='boolean',
        description=''
    )

    ca_that_registration_date = ColumnSpec(
        data_type='timestamp',
        description=''
    )

    ca_that_unsubscribe_date = ColumnSpec(
        data_type='timestamp',
        description=''
    )

    ca_that_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_threshold = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_time = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_time_zone = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_title = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_top_banner_image_url = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_top_banner_url = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_total_accrued = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_total_order_value = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_total_redeemed = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_total_revenue = ColumnSpec(
        data_type='decimal(11,3)',
        description=''
    )

    ca_trigger_antichurn_trigger = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_trigger_coupon_use_trigger = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_trigger_onboardingyourcrf_trigger = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_trigger_onboarding_coupon = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_type = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_ucg_group = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_ucg_utg_tl_flag = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_uninstalled_at = ColumnSpec(
        data_type='timestamp',
        description=''
    )

    ca_unique_link = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_unique_link_test = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_uplift_campaigns = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_user_aliases = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_user_privacy_blacklist_status = ColumnSpec(
        data_type='boolean',
        description=''
    )

    ca_utg_group = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_vox_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_vox_registration_date = ColumnSpec(
        data_type='timestamp',
        description=''
    )

    ca_vox_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_vox_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_vt_store_name = ColumnSpec(
        data_type='binary',
        description=''
    )

    ca_whatsapp_city = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_whatsapp_country = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_whatsapp_name = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_whatsapp_preferred_language = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_whatsapp_subscribe_status = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_your_attribute = ColumnSpec(
        data_type='string',
        description=''
    )

    ca_dat_batch = ColumnSpec(
        data_type='date',
        description=''
    )
