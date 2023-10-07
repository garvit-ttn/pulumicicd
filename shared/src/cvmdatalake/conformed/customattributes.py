from enum import Enum
from cvmdatalake import TableSpec, ColumnSpec


class CustomAttributes(TableSpec):

    @classmethod
    def table_description(cls):
        return "Input Activities data provided by MAF"

    smbu_total_spend_fashion_for_12_months_138 = ColumnSpec(
        data_type='string',
        description=''
    )

    smbu_total_spend_electronics_for_12_months_139 = ColumnSpec(
        data_type='string',
        description=''
    )

    smbu_total_spend_luxury_for_12_months_140 = ColumnSpec(
        data_type='string',
        description=''
    )

    smbu_total_spend_fnb_for_12_months_141 = ColumnSpec(
        data_type='string',
        description=''
    )

    smbu_total_spend_luxuryfashion_for_12_months_142 = ColumnSpec(
        data_type='string',
        description=''
    )

    smbu_total_spend_furniture_household_for_12_months_143 = ColumnSpec(
        data_type='string',
        description=''
    )

    smbu_total_spend_sports_for_12_months_144 = ColumnSpec(
        data_type='string',
        description=''
    )

    smbu_avg_spend_fashion_for_12_months_148 = ColumnSpec(
        data_type='string',
        description=''
    )

    smbu_avg_spend_electronics_for_12_months_149 = ColumnSpec(
        data_type='string',
        description=''
    )

    smbu_avg_spend_luxury_for_12_months_150 = ColumnSpec(
        data_type='string',
        description=''
    )

    smbu_avg_spend_fnb_for_12_months_151 = ColumnSpec(
        data_type='string',
        description=''
    )

    smbu_avg_spend_luxuryfashion_for_12_months_152 = ColumnSpec(
        data_type='string',
        description=''
    )

    smbu_avg_spend_furniture_household_for_12_months_153 = ColumnSpec(
        data_type='string',
        description=''
    )

    smbu_avg_spend_sports_for_12_months_154 = ColumnSpec(
        data_type='string',
        description=''
    )

    vox_avg_txn_value_for_12_months_159 = ColumnSpec(
        data_type='string',
        description=''
    )

    vox_total_spend_for_12_months_164 = ColumnSpec(
        data_type='string',
        description=''
    )

    vox_arabic_movie_count_169 = ColumnSpec(
        data_type='string',
        description=''
    )

    vox_hindi_movie_count_170 = ColumnSpec(
        data_type='string',
        description=''
    )

    vox_childrens_movie_count_172 = ColumnSpec(
        data_type='string',
        description=''
    )

    vox_cinema_total_spend_last_12_months_175 = ColumnSpec(
        data_type='string',
        description=''
    )

    vox_cinema_total_spend_last_6_months_176 = ColumnSpec(
        data_type='string',
        description=''
    )

    vox_cinema_total_spend_last_3_months_177 = ColumnSpec(
        data_type='string',
        description=''
    )

    maf_cinema_total_txn_last_12_months_180 = ColumnSpec(
        data_type='string',
        description=''
    )

    maf_cinema_total_txn_last_6_months_181 = ColumnSpec(
        data_type='string',
        description=''
    )

    maf_cinema_total_txn_last_3_months_182 = ColumnSpec(
        data_type='string',
        description=''
    )

    vox_total_number_transactions_191 = ColumnSpec(
        data_type='string',
        description=''
    )

    vox_family_movie_total_revenue_199 = ColumnSpec(
        data_type='string',
        description=''
    )

    crf_total_spend_last_12_months_1 = ColumnSpec(
        data_type='string',
        description=''
    )

    expiring_points_20230331 = ColumnSpec(
        data_type='binary',
        description=''
    )

    exp_points_worth_aed_20230331 = ColumnSpec(
        data_type='binary',
        description=''
    )

    q2_target_control2023 = ColumnSpec(
        data_type='string',
        description=''
    )

    q2_target_control_filter2023 = ColumnSpec(
        data_type='binary',
        description=''
    )

    crf_last_transaction_date_20 = ColumnSpec(
        data_type='string',
        description=''
    )

    vox_family_movie_total_txn_214 = ColumnSpec(
        data_type='string',
        description=''
    )

    vox_scifi_movie_total_txn_223 = ColumnSpec(
        data_type='string',
        description=''
    )

    vox_arabic_movie_total_txn_231 = ColumnSpec(
        data_type='string',
        description=''
    )

    vox_kids_movie_total_txn_236 = ColumnSpec(
        data_type='string',
        description=''
    )

    vox_preferred_mall_237 = ColumnSpec(
        data_type='string',
        description=''
    )

    crf_preferred_store_23 = ColumnSpec(
        data_type='string',
        description=''
    )

    vox_most_recent_movie_watched_244 = ColumnSpec(
        data_type='string',
        description=''
    )

    vox_number_movies_last_1_month_245 = ColumnSpec(
        data_type='string',
        description=''
    )

    lne_avg_txn_for_12_months_246 = ColumnSpec(
        data_type='string',
        description=''
    )

    lne_no_txn_for_12_months_247 = ColumnSpec(
        data_type='string',
        description=''
    )

    crf_preferred_city_24 = ColumnSpec(
        data_type='string',
        description=''
    )

    lne_total_spend_for_12_months_250 = ColumnSpec(
        data_type='string',
        description=''
    )

    lne_total_transactions_last_6_months_258 = ColumnSpec(
        data_type='string',
        description=''
    )

    lne_total_trransactions_last_3_months_259 = ColumnSpec(
        data_type='string',
        description=''
    )

    lne_avg_txn_for_12_months_267 = ColumnSpec(
        data_type='string',
        description=''
    )

    crf_lifecycle_segment_26 = ColumnSpec(
        data_type='string',
        description=''
    )

    lec_total_spend_for_12_months_271 = ColumnSpec(
        data_type='string',
        description=''
    )

    lec_mp_avg_txn_value_for_12_months_289 = ColumnSpec(
        data_type='string',
        description=''
    )

    ski_total_txn_last_6_months_301 = ColumnSpec(
        data_type='string',
        description=''
    )

    ski_total_txn_last_3_months_302 = ColumnSpec(
        data_type='string',
        description=''
    )

    crf_myclub_registration_date_30 = ColumnSpec(
        data_type='string',
        description=''
    )

    crf_member_status_32 = ColumnSpec(
        data_type='string',
        description=''
    )

    crf_member_shopper_group_segment_35 = ColumnSpec(
        data_type='string',
        description=''
    )

    crf_member_shopper_group_segment_country_35 = ColumnSpec(
        data_type='string',
        description=''
    )

    lifestyle_avg_txn_value_1_year_373 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_freq_of_txn_1_year_374 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_no_of_days_of_visit_1_year_375 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_last_trans_date_376 = ColumnSpec(
        data_type='timestamp',
        description=''
    )

    lifestyle_total_spend_1_year_377 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_total_spend_3_months_378 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_total_spend_6_months_379 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_total_spend_lifecycle_380 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_transaction_last_180_days_381 = ColumnSpec(
        data_type='boolean',
        description=''
    )

    lifestyle_months_dormant_382 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_total_spend_2_months_383 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_total_spend_1_month_384 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_total_txn_last_last_6_months_385 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_total_txn_last_3_months_386 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_total_txn_last_2_months_387 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_total_txn_last_1_month_388 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_avg_spend_last_6_months_389 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    crf_coupon_redeemed_38 = ColumnSpec(
        data_type='string',
        description=''
    )

    lifestyle_avg_spend_last_3_months_390 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_avg_spend_last_2_months_391 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_avg_spend_last_1_month_392 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    crf_last_instore_txn_40 = ColumnSpec(
        data_type='string',
        description=''
    )

    crf_supermarket_last_txn_42 = ColumnSpec(
        data_type='string',
        description=''
    )

    crf_hypermarket_last_txn_43 = ColumnSpec(
        data_type='string',
        description=''
    )

    crf_moemirates_last_txn_44 = ColumnSpec(
        data_type='string',
        description=''
    )

    crf_ccderia_last_txn_45 = ColumnSpec(
        data_type='string',
        description=''
    )

    crf_burjuman_last_txn_46 = ColumnSpec(
        data_type='string',
        description=''
    )

    crf_ibnbattuta_last_txn_47 = ColumnSpec(
        data_type='string',
        description=''
    )

    crf_ccsharjah_last_txn_48 = ColumnSpec(
        data_type='string',
        description=''
    )

    crf_abudhabi_airportroad_last_txn_49 = ColumnSpec(
        data_type='string',
        description=''
    )

    crf_ccfujairah_last_txn_50 = ColumnSpec(
        data_type='string',
        description=''
    )

    crf_rakmanar_last_txn_51 = ColumnSpec(
        data_type='string',
        description=''
    )

    crf_total_txn_instore_last_3_months_52 = ColumnSpec(
        data_type='string',
        description=''
    )

    smbu_mall_aquisition_572 = ColumnSpec(
        data_type='string',
        description=''
    )

    smbu_mall_visit_frequency_last_12_months_573 = ColumnSpec(
        data_type='string',
        description=''
    )

    smbu_mall_visit_frequency_last_6_months_574 = ColumnSpec(
        data_type='string',
        description=''
    )

    share_active_30_days_575 = ColumnSpec(
        data_type='string',
        description=''
    )

    share_active_60_days_576 = ColumnSpec(
        data_type='string',
        description=''
    )

    share_active_90_days_577 = ColumnSpec(
        data_type='string',
        description=''
    )

    share_active_180_days_578 = ColumnSpec(
        data_type='string',
        description=''
    )

    lifestyle_furniture_category_spend_last_1_year_580 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_decor_category_spend_last_1_year_581 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_kids_category_spend_last_1_year_582 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    crf_first_transaction_date_668 = ColumnSpec(
        data_type='string',
        description=''
    )

    lifestyle_furniture_category_spend_last_3_months_674 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_furniture_category_spend_last_1_month_675 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_furniture_category_spend_last_7_days_676 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    crf_consumergood_instore_spend_last_3_months_67 = ColumnSpec(
        data_type='string',
        description=''
    )

    lifestyle_decor_category_spend_last_3_months_680 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_decor_category_spend_last_1_month_681 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_decor_category_spend_last_7_days_682 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_kids_category_spend_last_3_months_686 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_kids_category_spend_last_1_month_687 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_kids_category_spend_last_7_days_688 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    crf_consumergood_instore_spend_last_3_months_68 = ColumnSpec(
        data_type='string',
        description=''
    )

    lifestyle_accessories_category_spend_last_3_months_690 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_accessories_category_spend_last_1_month_691 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_accessories_category_spend_last_7_days_692 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_jewelry_category_spend_last_3_months_697 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_jewelry_category_spend_last_1_month_698 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_jewelry_category_spend_last_7_days_699 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    crf_heavyhousehold_instore_spend_last_3_months_69 = ColumnSpec(
        data_type='string',
        description=''
    )

    lifestyle_fitness_clothing_category_spend_last_3_months_704 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_fitness_clothing_category_spend_last_1_month_705 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_fitness_clothing_category_spend_last_7_days_706 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    crf_lighthousehold_instore_spend_last_3_months_70 = ColumnSpec(
        data_type='string',
        description=''
    )

    lifestyle_fitness_equipment_category_spend_last_3_months_711 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_fitness_equipment_category_spend_last_1_month_712 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_fitness_equipment_category_spend_last_7_days_713 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_lego_duplo_category_spend_last_3_months_718 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_lego_duplo_category_spend_last_1_month_719 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    crf_textiles_instore_spend_last_3_months_71 = ColumnSpec(
        data_type='string',
        description=''
    )

    lifestyle_lego_duplo_category_spend_last_7_days_720 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_run_category_spend_last_3_months_725 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_run_category_spend_last_1_month_726 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_run_category_spend_last_7_days_727 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    crf_consumergood_no_txn_instore_last_3_months_72 = ColumnSpec(
        data_type='string',
        description=''
    )

    lifestyle_yoga_category_spend_last_3_months_732 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_yoga_category_spend_last_1_month_733 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_yoga_category_spend_last_7_days_734 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_lego_kids_category_spend_last_3_months_739 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    crf_freshfood_no_txn_instore_last_3_months_73 = ColumnSpec(
        data_type='string',
        description=''
    )

    lifestyle_lego_kids_category_spend_last_1_month_740 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_lego_kids_category_spend_last_7_days_741 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_lego_adults_category_spend_last_3_months_746 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_lego_adults_category_spend_last_1_month_747 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_lego_adults_category_spend_last_7_days_748 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    crf_heavyhousehold_no_txn_instore_last_3_months_74 = ColumnSpec(
        data_type='string',
        description=''
    )

    crf_lighthousehold_no_txn_instore_last_3_months_75 = ColumnSpec(
        data_type='string',
        description=''
    )

    crf_textiles_no_txn_instore_last_3_months_76 = ColumnSpec(
        data_type='string',
        description=''
    )

    lifestyle_footwear_category_spend_last_3_months_878 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_footwear_category_spend_last_1_month_879 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_footwear_category_spend_last_7_days_880 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_transaction_frequency_moe_last_3_months_881 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_transaction_frequency_moe_last_1_month_882 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_transaction_frequency_ccmi_last_3_months_883 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_transaction_frequency_ccmi_last_1_month_884 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    offers_activated_3m_885 = ColumnSpec(
        data_type='string',
        description=''
    )

    offers_activated_6m_886 = ColumnSpec(
        data_type='string',
        description=''
    )

    offers_activated_lifetime_887 = ColumnSpec(
        data_type='string',
        description=''
    )

    receipt_scanning_failed_lifetime_888 = ColumnSpec(
        data_type='string',
        description=''
    )

    receipt_scanning_failed_6m_889 = ColumnSpec(
        data_type='string',
        description=''
    )

    receipt_scanning_failed_3m_890 = ColumnSpec(
        data_type='string',
        description=''
    )

    receipt_scanning_failed_1m_891 = ColumnSpec(
        data_type='string',
        description=''
    )

    receipt_scanning_failed_c4_lifetime_892 = ColumnSpec(
        data_type='string',
        description=''
    )

    receipt_scanning_failed_c4_6m_893 = ColumnSpec(
        data_type='string',
        description=''
    )

    receipt_scanning_failed_c4_3m_894 = ColumnSpec(
        data_type='string',
        description=''
    )

    receipt_scanning_failed_c4_1m_895 = ColumnSpec(
        data_type='string',
        description=''
    )

    receipt_scanning_failed_c4_7d_896 = ColumnSpec(
        data_type='string',
        description=''
    )

    receipt_scanning_succeeded_lifetime_897 = ColumnSpec(
        data_type='string',
        description=''
    )

    receipt_scanning_succeeded_6m_898 = ColumnSpec(
        data_type='string',
        description=''
    )

    receipt_scanning_succeeded_3m_899 = ColumnSpec(
        data_type='string',
        description=''
    )

    receipt_scanning_succeeded_1m_900 = ColumnSpec(
        data_type='string',
        description=''
    )

    share_pay_active_3m_901 = ColumnSpec(
        data_type='string',
        description=''
    )

    share_pay_active_6m_902 = ColumnSpec(
        data_type='string',
        description=''
    )

    share_pay_active_lifetime_903 = ColumnSpec(
        data_type='string',
        description=''
    )

    date_of_first_earn_904 = ColumnSpec(
        data_type='string',
        description=''
    )

    date_of_last_earn_905 = ColumnSpec(
        data_type='string',
        description=''
    )

    date_of_first_redemption_906 = ColumnSpec(
        data_type='string',
        description=''
    )

    date_of_last_redemption_907 = ColumnSpec(
        data_type='string',
        description=''
    )

    count_of_sharepay_earn_lifetime_908 = ColumnSpec(
        data_type='string',
        description=''
    )

    count_of_sharepay_earn_7d_909 = ColumnSpec(
        data_type='string',
        description=''
    )

    count_of_share_id_earn_lifetime_910 = ColumnSpec(
        data_type='string',
        description=''
    )

    count_of_share_id_earn_7d_911 = ColumnSpec(
        data_type='string',
        description=''
    )

    lifestyle_total_luxury_spend_last_3_months_912 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_total_luxury_spend_last_1_month_913 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_total_luxury_spend_last_7_days_914 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_holiday_category_spend_last_3_months_915 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_holiday_category_spend_last_1_month_916 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_holiday_category_spend_last_7_days_917 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_clothing_category_spend_last_3_months_918 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_clothing_category_spend_last_1_month_919 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_essential_fashion_category_spend_last_3_months_920 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_essential_fashion_category_spend_last_1_month_921 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_visit_frequency_moe_last_3_months_926 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_visit_frequency_moe_last_1_month_927 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_visit_frequency_ccmi_last_3_months_928 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_visit_frequency_ccmi_last_1_month_929 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    smbu_total_spend_luxury_for_1_month_938 = ColumnSpec(
        data_type='string',
        description=''
    )

    smbu_total_spend_luxury_for_3_months_939 = ColumnSpec(
        data_type='string',
        description=''
    )

    smbu_total_spend_luxury_for_6_months_940 = ColumnSpec(
        data_type='string',
        description=''
    )

    lifestyle_luxury_spend_but_not_in_that_last_3_months_946 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_luxury_spend_but_not_in_that_last_1_month_947 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_gift_card_trx_count_last_3_months_948 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_share_point_redemption_count_last_3_months_949 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_promocode_redemption_count_last_3_months_950 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_promocode_redemption_count_last_1_month_951 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_share_point_redemption_count_last_1_month_952 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_kitchen_category_spend_last_3_months_953 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_kitchen_category_spend_last_1_month_954 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_kitchen_category_spend_last_7_days_955 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_luxury_spend_but_not_in_als_last_3_months_956 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_luxury_spend_but_not_in_als_last_1_month_957 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_furniture_spend_but_not_in_cb_last_3_months_958 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_furniture_spend_but_not_in_cb_last_1_month_959 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_furniture_spend_but_not_in_cb2_last_3_months_960 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lifestyle_furniture_spend_but_not_in_cb2_last_1_month_961 = ColumnSpec(
        data_type='bigint',
        description=''
    )

    abercrombiefitch_email_subscribe = ColumnSpec(
        data_type='boolean',
        description=''
    )

    abercrombiefitch_unsubscribe_date = ColumnSpec(
        data_type='timestamp',
        description=''
    )

    abercrombiefitch_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    allsaints_email_subscribe = ColumnSpec(
        data_type='boolean',
        description=''
    )

    allsaints_registration_date = ColumnSpec(
        data_type='timestamp',
        description=''
    )

    allsaints_unsubscribe_date = ColumnSpec(
        data_type='timestamp',
        description=''
    )

    allsaints_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    apps = ColumnSpec(
        data_type='binary',
        description=''
    )

    app_country = ColumnSpec(
        data_type='string',
        description=''
    )

    app_id = ColumnSpec(
        data_type='string',
        description=''
    )

    app_language = ColumnSpec(
        data_type='string',
        description=''
    )

    april_tp_offer_link = ColumnSpec(
        data_type='binary',
        description=''
    )

    april_tp_offer_link_update = ColumnSpec(
        data_type='binary',
        description=''
    )

    april_tp_offer_link_update_j2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    april_tp_spend_threshold = ColumnSpec(
        data_type='binary',
        description=''
    )

    april_tp_spend_threshold_j2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    area_code = ColumnSpec(
        data_type='string',
        description=''
    )

    automated_preferredstores_trigger = ColumnSpec(
        data_type='string',
        description=''
    )

    automated_test_low_extra_low_trigger = ColumnSpec(
        data_type='string',
        description=''
    )

    a_like = ColumnSpec(
        data_type='string',
        description=''
    )

    babies_products = ColumnSpec(
        data_type='string',
        description=''
    )

    big_fish_share_of_wallet = ColumnSpec(
        data_type='binary',
        description=''
    )

    bio_products = ColumnSpec(
        data_type='string',
        description=''
    )

    bottom_banner_image_url = ColumnSpec(
        data_type='string',
        description=''
    )

    bought_xy_products = ColumnSpec(
        data_type='string',
        description=''
    )

    nam_brand = ColumnSpec(
        data_type='string',
        description=''
    )

    braze_external_id = ColumnSpec(
        data_type='string',
        description=''
    )

    braze_id = ColumnSpec(
        data_type='string',
        description=''
    )

    c4_bahrain_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    c4_bahrain_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    c4_bahrain_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    c4_egypt_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    c4_egypt_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    c4_egypt_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    c4_georgia_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    c4_georgia_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    c4_georgia_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    c4_grg_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    c4_grg_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    c4_grg_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    c4_jordan_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    c4_jordan_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    c4_jordan_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    c4_kenya_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    c4_kenya_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    c4_kenya_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    c4_ksa_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    c4_ksa_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    c4_ksa_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    c4_kuwait_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    c4_kuwait_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    c4_kuwait_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    c4_lebanon_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    c4_lebanon_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    c4_lebanon_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    c4_oman_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    c4_oman_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    c4_oman_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    c4_pakistan_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    c4_pakistan_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    c4_pakistan_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    c4_push_marketing_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    c4_push_myorders_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    c4_qatar_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    c4_qatar_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    c4_qatar_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    c4_uae_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    c4_uae_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    c4_uae_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    c4_uzbekistan_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    c4_uzbekistan_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    c4_uzbekistan_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    campaign_test_mail = ColumnSpec(
        data_type='binary',
        description=''
    )

    carrefour_registration_date = ColumnSpec(
        data_type='timestamp',
        description=''
    )

    cart_count = ColumnSpec(
        data_type='string',
        description=''
    )

    category_level2_typ_reco_cgd_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    category_products = ColumnSpec(
        data_type='string',
        description=''
    )

    cb2_email_subscribe = ColumnSpec(
        data_type='boolean',
        description=''
    )

    cb2_registration_date = ColumnSpec(
        data_type='timestamp',
        description=''
    )

    cb2_unsubscribe_date = ColumnSpec(
        data_type='timestamp',
        description=''
    )

    cb2_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    cbu_registration_date = ColumnSpec(
        data_type='timestamp',
        description=''
    )

    cb_email_subscribe = ColumnSpec(
        data_type='boolean',
        description=''
    )

    cb_registration_date = ColumnSpec(
        data_type='timestamp',
        description=''
    )

    ccmi_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    ccmi_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ccmi_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    cc_ajman_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    cc_ajman_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    cc_ajman_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    cc_alexandria_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    cc_alexandria_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    cc_alexandria_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    cc_almaza_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    cc_almaza_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    cc_almaza_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    cc_alshindagha_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    cc_alshindagha_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    cc_alshindagha_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    cc_alzahia_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    cc_alzahia_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    cc_alzahia_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    cc_bahrain_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    cc_bahrain_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    cc_bahrain_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    cc_deira_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    cc_deira_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    cc_deira_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    cc_fujairah_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    cc_fujairah_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    cc_fujairah_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    cc_maadi_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    cc_maadi_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    cc_maadi_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    cc_mall_code = ColumnSpec(
        data_type='string',
        description=''
    )

    cc_meaisem_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    cc_meaisem_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    cc_meaisem_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    cc_mirdiff_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    cc_mirdiff_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    cc_mirdiff_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    cc_muscat_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    cc_muscat_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    cc_muscat_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    cc_qurum_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    cc_qurum_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    cc_qurum_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    cc_sharjah_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    cc_sharjah_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    cc_sharjah_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    cc_suhar_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    cc_suhar_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    cc_suhar_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    churn_prop = ColumnSpec(
        data_type='string',
        description=''
    )

    cod_sor_counterparty = ColumnSpec(
        data_type='string',
        description=''
    )

    cod_sor_gr = ColumnSpec(
        data_type='string',
        description=''
    )

    nam_country = ColumnSpec(
        data_type='string',
        description=''
    )

    cde_country_of_residence = ColumnSpec(
        data_type='string',
        description=''
    )

    coupon_generated = ColumnSpec(
        data_type='string',
        description=''
    )

    coupon_1 = ColumnSpec(
        data_type='string',
        description=''
    )

    coupon_2 = ColumnSpec(
        data_type='string',
        description=''
    )

    coupon_code = ColumnSpec(
        data_type='string',
        description=''
    )

    cratebarrelzzz_email_subscribe = ColumnSpec(
        data_type='boolean',
        description=''
    )

    cratebarrelzzz_unsubscribe_date = ColumnSpec(
        data_type='timestamp',
        description=''
    )

    cratebarrel_email_subscribe = ColumnSpec(
        data_type='boolean',
        description=''
    )

    cratebarrel_registration_date = ColumnSpec(
        data_type='timestamp',
        description=''
    )

    cratebarrel_unsubscribe_date = ColumnSpec(
        data_type='timestamp',
        description=''
    )

    cratebarrel_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    created_at = ColumnSpec(
        data_type='string',
        description=''
    )

    customer_email = ColumnSpec(
        data_type='string',
        description=''
    )

    des_language_customer = ColumnSpec(
        data_type='string',
        description=''
    )

    customer_title = ColumnSpec(
        data_type='string',
        description=''
    )

    custom_date = ColumnSpec(
        data_type='string',
        description=''
    )

    custom_events = ColumnSpec(
        data_type='binary',
        description=''
    )

    custom_property = ColumnSpec(
        data_type='string',
        description=''
    )

    custom_property1 = ColumnSpec(
        data_type='string',
        description=''
    )

    custom_property2 = ColumnSpec(
        data_type='string',
        description=''
    )

    custom_proprty1 = ColumnSpec(
        data_type='string',
        description=''
    )

    cvm_audience = ColumnSpec(
        data_type='binary',
        description=''
    )

    dat_birth = ColumnSpec(
        data_type='timestamp',
        description=''
    )

    devices = ColumnSpec(
        data_type='binary',
        description=''
    )

    discount = ColumnSpec(
        data_type='string',
        description=''
    )

    discount_alert_products = ColumnSpec(
        data_type='string',
        description=''
    )

    dla_audience_membership = ColumnSpec(
        data_type='binary',
        description=''
    )

    dla_file_upload_membership = ColumnSpec(
        data_type='binary',
        description=''
    )

    dob = ColumnSpec(
        data_type='timestamp',
        description=''
    )

    dreamscape_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    dreamscape_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    dreamscape_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    email = ColumnSpec(
        data_type='string',
        description=''
    )

    email_id = ColumnSpec(
        data_type='string',
        description=''
    )

    email_items = ColumnSpec(
        data_type='string',
        description=''
    )

    email_verified = ColumnSpec(
        data_type='boolean',
        description=''
    )

    email_verified_date = ColumnSpec(
        data_type='timestamp',
        description=''
    )

    dat_enrollment = ColumnSpec(
        data_type='string',
        description=''
    )

    nam_enrollment_sponsor = ColumnSpec(
        data_type='string',
        description=''
    )

    enrolment_date = ColumnSpec(
        data_type='string',
        description=''
    )

    eow_5xreminder_trigger = ColumnSpec(
        data_type='string',
        description=''
    )

    eow_butcherypoultryonlyoverall_trigger = ColumnSpec(
        data_type='string',
        description=''
    )

    eow_butcherypoultryonlypromo_trigger = ColumnSpec(
        data_type='string',
        description=''
    )

    eow_dairy_trigger = ColumnSpec(
        data_type='string',
        description=''
    )

    eow_dphallproducts_trigger = ColumnSpec(
        data_type='string',
        description=''
    )

    eow_electronicsallproducts_trigger = ColumnSpec(
        data_type='string',
        description=''
    )

    eow_fballproducts_trigger = ColumnSpec(
        data_type='string',
        description=''
    )

    eow_fbmasspromo_underreached = ColumnSpec(
        data_type='binary',
        description=''
    )

    eow_festivepreorder_trigger = ColumnSpec(
        data_type='string',
        description=''
    )

    eow_fruitsveggiesallproducts_trigger = ColumnSpec(
        data_type='string',
        description=''
    )

    eow_fruitsveggies_trigger = ColumnSpec(
        data_type='string',
        description=''
    )

    eow_fruitsvegproductsonlypromo_trigger = ColumnSpec(
        data_type='string',
        description=''
    )

    eow_grocerypromo_trigger = ColumnSpec(
        data_type='string',
        description=''
    )

    eow_grocery_trigger = ColumnSpec(
        data_type='string',
        description=''
    )

    eow_household_trigger = ColumnSpec(
        data_type='string',
        description=''
    )

    eow_nuts_trigger = ColumnSpec(
        data_type='string',
        description=''
    )

    eow_outdoor_trigger = ColumnSpec(
        data_type='string',
        description=''
    )

    eow_recononfood_underreached = ColumnSpec(
        data_type='binary',
        description=''
    )

    eow_reco_butcherypoultryonlyoverall_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    eow_reco_butcherypoultryonlypromo_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    eow_reco_dairy_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    eow_reco_dphallproducts_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    eow_reco_electronicsallproducts_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    eow_reco_fballproducts_beverage = ColumnSpec(
        data_type='binary',
        description=''
    )

    eow_reco_fballproducts_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    eow_reco_fballproducts_grocery = ColumnSpec(
        data_type='binary',
        description=''
    )

    eow_reco_fbmasspromo = ColumnSpec(
        data_type='binary',
        description=''
    )

    eow_reco_fbmasspromo2_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    eow_reco_fbmasspromo_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    eow_reco_fruitsveggiesallproducts_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    eow_reco_fruitsveggies_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    eow_reco_fruitsvegproductsonlypromo_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    eow_reco_grocerypromo_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    eow_reco_grocery_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    eow_reco_household_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    eow_reco_nuts_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    eow_reco_outdoor_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    eow_reco_outdoor_outdoor_grill = ColumnSpec(
        data_type='binary',
        description=''
    )

    eow_reco_outdoor_outdoor_others = ColumnSpec(
        data_type='binary',
        description=''
    )

    eow_reco_recononfood = ColumnSpec(
        data_type='binary',
        description=''
    )

    eow_reco_toys_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    eow_reco_ultrafreshallproducts_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    eow_share5x_trigger = ColumnSpec(
        data_type='string',
        description=''
    )

    eow_test_attribute = ColumnSpec(
        data_type='binary',
        description=''
    )

    eow_test_attribute_2 = ColumnSpec(
        data_type='string',
        description=''
    )

    eow_test_attribute_3 = ColumnSpec(
        data_type='string',
        description=''
    )

    eow_toys_trigger = ColumnSpec(
        data_type='string',
        description=''
    )

    eow_ultrafreshallproducts_trigger = ColumnSpec(
        data_type='string',
        description=''
    )

    external_id = ColumnSpec(
        data_type='string',
        description=''
    )

    fab_cc_issue_date = ColumnSpec(
        data_type='string',
        description=''
    )

    fab_cobrand_product = ColumnSpec(
        data_type='string',
        description=''
    )

    fab_customer_id = ColumnSpec(
        data_type='string',
        description=''
    )

    fashion4less_email_subscribe = ColumnSpec(
        data_type='boolean',
        description=''
    )

    fashion4less_unsubscribe_date = ColumnSpec(
        data_type='timestamp',
        description=''
    )

    fashion4less_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    favfruit = ColumnSpec(
        data_type='string',
        description=''
    )

    favorite_category = ColumnSpec(
        data_type='string',
        description=''
    )

    feed_me_labneh_ids = ColumnSpec(
        data_type='string',
        description=''
    )

    firstname_gcr = ColumnSpec(
        data_type='string',
        description=''
    )

    first_fab_earn_date = ColumnSpec(
        data_type='string',
        description=''
    )

    dat_first_login = ColumnSpec(
        data_type='string',
        description=''
    )

    first_name = ColumnSpec(
        data_type='string',
        description=''
    )

    dat_first_login_share = ColumnSpec(
        data_type='string',
        description=''
    )

    flag_abd_store = ColumnSpec(
        data_type='string',
        description=''
    )

    flag_dub_store = ColumnSpec(
        data_type='string',
        description=''
    )

    fnb_als = ColumnSpec(
        data_type='string',
        description=''
    )

    fnb_als_topsellers = ColumnSpec(
        data_type='string',
        description=''
    )

    fnb_cross_sell = ColumnSpec(
        data_type='string',
        description=''
    )

    fnb_cross_sell_als = ColumnSpec(
        data_type='string',
        description=''
    )

    fnb_frequency_not_promo_products = ColumnSpec(
        data_type='string',
        description=''
    )

    fnb_frequency_products = ColumnSpec(
        data_type='string',
        description=''
    )

    fnb_topsellers = ColumnSpec(
        data_type='string',
        description=''
    )

    gender = ColumnSpec(
        data_type='string',
        description=''
    )

    globalsnow_booking_visit_date = ColumnSpec(
        data_type='binary',
        description=''
    )

    globalsnow_language = ColumnSpec(
        data_type='string',
        description=''
    )

    globalsnow_website_language = ColumnSpec(
        data_type='string',
        description=''
    )

    global_snow_next_visit_date = ColumnSpec(
        data_type='string',
        description=''
    )

    gulf_news = ColumnSpec(
        data_type='string',
        description=''
    )

    happiness_lab_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    happiness_lab_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    hollister_email_subscribe = ColumnSpec(
        data_type='boolean',
        description=''
    )

    hollister_unsubscribe_date = ColumnSpec(
        data_type='timestamp',
        description=''
    )

    hollister_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    home_city = ColumnSpec(
        data_type='string',
        description=''
    )

    hv_end_date = ColumnSpec(
        data_type='string',
        description=''
    )

    hv_enrolment_date = ColumnSpec(
        data_type='string',
        description=''
    )

    hv_optin_status = ColumnSpec(
        data_type='string',
        description=''
    )

    hv_segment = ColumnSpec(
        data_type='string',
        description=''
    )

    hv_tier = ColumnSpec(
        data_type='string',
        description=''
    )

    iamai_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    iamai_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    iamai_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    ide_email = ColumnSpec(
        data_type='string',
        description=''
    )

    idfa_setting = ColumnSpec(
        data_type='string',
        description=''
    )

    idi_counterparty = ColumnSpec(
        data_type='string',
        description=''
    )

    ifly_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    ifly_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    ifly_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    isd_code = ColumnSpec(
        data_type='bigint',
        description=''
    )

    item_order_value = ColumnSpec(
        data_type='string',
        description=''
    )

    ken_18_plus = ColumnSpec(
        data_type='string',
        description=''
    )

    des_language = ColumnSpec(
        data_type='string',
        description=''
    )

    lastname_gcr = ColumnSpec(
        data_type='string',
        description=''
    )

    dat_last_accrual = ColumnSpec(
        data_type='string',
        description=''
    )

    dat_last_activity = ColumnSpec(
        data_type='string',
        description=''
    )

    last_coordinates = ColumnSpec(
        data_type='binary',
        description=''
    )

    dat_last_login = ColumnSpec(
        data_type='string',
        description=''
    )

    last_name = ColumnSpec(
        data_type='string',
        description=''
    )

    dat_last_redemption = ColumnSpec(
        data_type='string',
        description=''
    )

    last_viewed_page = ColumnSpec(
        data_type='string',
        description=''
    )

    latitude = ColumnSpec(
        data_type='string',
        description=''
    )

    lego_email_subscribe = ColumnSpec(
        data_type='boolean',
        description=''
    )

    lego_registration_date = ColumnSpec(
        data_type='timestamp',
        description=''
    )

    lego_unsubscribe_date = ColumnSpec(
        data_type='timestamp',
        description=''
    )

    lego_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    lifecycle = ColumnSpec(
        data_type='string',
        description=''
    )

    littleexplorers_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    littleexplorers_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    littleexplorers_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    loginfrom = ColumnSpec(
        data_type='binary',
        description=''
    )

    login_status = ColumnSpec(
        data_type='string',
        description=''
    )

    longitude = ColumnSpec(
        data_type='string',
        description=''
    )

    lululemon_email_subscribe = ColumnSpec(
        data_type='boolean',
        description=''
    )

    lululemon_registration_date = ColumnSpec(
        data_type='timestamp',
        description=''
    )

    lululemon_unsubscribe_date = ColumnSpec(
        data_type='timestamp',
        description=''
    )

    lululemon_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    mafsb_campaigns = ColumnSpec(
        data_type='binary',
        description=''
    )

    mafsb_churned = ColumnSpec(
        data_type='string',
        description=''
    )

    mafsb_groups = ColumnSpec(
        data_type='binary',
        description=''
    )

    magicplanet_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    magicplanet_registration_date = ColumnSpec(
        data_type='timestamp',
        description=''
    )

    magicplanet_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    magicplanet_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    maisonsdumonde_email_subscribe = ColumnSpec(
        data_type='boolean',
        description=''
    )

    cde_mal = ColumnSpec(
        data_type='string',
        description=''
    )

    matajer_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    matajer_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    meal_ids = ColumnSpec(
        data_type='string',
        description=''
    )

    des_membership_stage = ColumnSpec(
        data_type='string',
        description=''
    )

    member_email = ColumnSpec(
        data_type='string',
        description=''
    )

    moegypt_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    moegypt_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    moegypt_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    moemirates_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    moemirates_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    moemirates_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    moe_language = ColumnSpec(
        data_type='string',
        description=''
    )

    moe_registration_date = ColumnSpec(
        data_type='timestamp',
        description=''
    )

    mooman_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    mooman_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    mooman_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    mycc_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    mycc_masdar_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    mycc_masdar_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    mycc_masdar_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    mycc_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    mycc_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    des_nationality = ColumnSpec(
        data_type='string',
        description=''
    )

    new_cust_flag = ColumnSpec(
        data_type='string',
        description=''
    )

    non_food_cross_sell = ColumnSpec(
        data_type='string',
        description=''
    )

    non_food_cross_sell_als = ColumnSpec(
        data_type='string',
        description=''
    )

    non_food_cross_sell_mba = ColumnSpec(
        data_type='string',
        description=''
    )

    non_food_cross_sell_promo = ColumnSpec(
        data_type='string',
        description=''
    )

    non_food_frequency = ColumnSpec(
        data_type='string',
        description=''
    )

    non_food_topsellers = ColumnSpec(
        data_type='string',
        description=''
    )

    non_opener_email = ColumnSpec(
        data_type='string',
        description=''
    )

    non_opener_push = ColumnSpec(
        data_type='string',
        description=''
    )

    nps_addtext = ColumnSpec(
        data_type='string',
        description=''
    )

    nps_consignment_code = ColumnSpec(
        data_type='string',
        description=''
    )

    nps_delivery_date = ColumnSpec(
        data_type='string',
        description=''
    )

    nps_item_count = ColumnSpec(
        data_type='string',
        description=''
    )

    nps_order_date = ColumnSpec(
        data_type='string',
        description=''
    )

    nps_order_number = ColumnSpec(
        data_type='string',
        description=''
    )

    nps_product_code = ColumnSpec(
        data_type='string',
        description=''
    )

    nps_product_url = ColumnSpec(
        data_type='string',
        description=''
    )

    nps_rating = ColumnSpec(
        data_type='string',
        description=''
    )

    nps_reasons = ColumnSpec(
        data_type='string',
        description=''
    )

    offer_link = ColumnSpec(
        data_type='binary',
        description=''
    )

    offline_first_maf_contact = ColumnSpec(
        data_type='string',
        description=''
    )

    order_count = ColumnSpec(
        data_type='string',
        description=''
    )

    order_date = ColumnSpec(
        data_type='string',
        description=''
    )

    order_id = ColumnSpec(
        data_type='string',
        description=''
    )

    organic_product_list = ColumnSpec(
        data_type='binary',
        description=''
    )

    otp_verified = ColumnSpec(
        data_type='string',
        description=''
    )

    persona = ColumnSpec(
        data_type='string',
        description=''
    )

    personalized_push_data = ColumnSpec(
        data_type='binary',
        description=''
    )

    phone = ColumnSpec(
        data_type='double',
        description=''
    )

    qty_points_balance = ColumnSpec(
        data_type='string',
        description=''
    )

    qty_points_won_spin_wheel = ColumnSpec(
        data_type='string',
        description=''
    )

    poltrona_frau_email_subscribe = ColumnSpec(
        data_type='boolean',
        description=''
    )

    poltrona_frau_unsubscribe_date = ColumnSpec(
        data_type='timestamp',
        description=''
    )

    poltrona_frau_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    preferred_store_name = ColumnSpec(
        data_type='string',
        description=''
    )

    previous_last_coordinates = ColumnSpec(
        data_type='string',
        description=''
    )

    productids_cross_sell = ColumnSpec(
        data_type='string',
        description=''
    )

    productids_default = ColumnSpec(
        data_type='string',
        description=''
    )

    productids_frequency = ColumnSpec(
        data_type='string',
        description=''
    )

    productids_up_sell = ColumnSpec(
        data_type='string',
        description=''
    )

    product_1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    product_2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    product_id = ColumnSpec(
        data_type='binary',
        description=''
    )

    product_ids = ColumnSpec(
        data_type='string',
        description=''
    )

    product_list = ColumnSpec(
        data_type='binary',
        description=''
    )

    product_list_1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    product_list_2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    product_list_3 = ColumnSpec(
        data_type='binary',
        description=''
    )

    product_list_33 = ColumnSpec(
        data_type='binary',
        description=''
    )

    product_list_t1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    product_list_t1_new = ColumnSpec(
        data_type='binary',
        description=''
    )

    product_t12_organic = ColumnSpec(
        data_type='binary',
        description=''
    )

    promo_code = ColumnSpec(
        data_type='string',
        description=''
    )

    promo_code_ar = ColumnSpec(
        data_type='string',
        description=''
    )

    promo_code_en = ColumnSpec(
        data_type='string',
        description=''
    )

    promo_for_churn_products = ColumnSpec(
        data_type='string',
        description=''
    )

    purchases = ColumnSpec(
        data_type='binary',
        description=''
    )

    purchase_channel = ColumnSpec(
        data_type='string',
        description=''
    )

    purchase_date = ColumnSpec(
        data_type='string',
        description=''
    )

    push_items = ColumnSpec(
        data_type='string',
        description=''
    )

    push_token = ColumnSpec(
        data_type='string',
        description=''
    )

    pv_product_list = ColumnSpec(
        data_type='binary',
        description=''
    )

    random_bucket = ColumnSpec(
        data_type='string',
        description=''
    )

    recent_categories = ColumnSpec(
        data_type='binary',
        description=''
    )

    recommendations = ColumnSpec(
        data_type='binary',
        description=''
    )

    recommendations_food = ColumnSpec(
        data_type='binary',
        description=''
    )

    recommendations_nonfood = ColumnSpec(
        data_type='binary',
        description=''
    )

    recommendations_non_food = ColumnSpec(
        data_type='string',
        description=''
    )

    recommendation_scores_grocery_2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    recommended_category = ColumnSpec(
        data_type='binary',
        description=''
    )

    reco_als = ColumnSpec(
        data_type='string',
        description=''
    )

    reco_topsellers = ColumnSpec(
        data_type='string',
        description=''
    )

    registeredfrom = ColumnSpec(
        data_type='string',
        description=''
    )

    registered_from = ColumnSpec(
        data_type='string',
        description=''
    )

    registration_date = ColumnSpec(
        data_type='binary',
        description=''
    )

    relationship_family_status = ColumnSpec(
        data_type='string',
        description=''
    )

    reward_points = ColumnSpec(
        data_type='binary',
        description=''
    )

    sales_boost_language = ColumnSpec(
        data_type='string',
        description=''
    )

    saved_card = ColumnSpec(
        data_type='string',
        description=''
    )

    sb_big_fish = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_churned_categories = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_3mpost_it = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_bakery_pastry = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_beverage = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_butchery = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_crayola = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_dairy_products_ss = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_elmers = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_faber_castel = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_fruits_vegetables = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_funbo = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_grocery = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_100percent20cus_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_20discount_04_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_abdudhabi_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_antichurn_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_april_1st_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_april_8_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_avataar_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_baby_1117_family = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_beautyt4_section = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_beauty_02_section = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_beauty_1115_section = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_beauty_1206_section = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_beauty_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_beauty_bigfish_section = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_beauty_new_section = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_beauty_section = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_bigdiscount_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_blending_family = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_cat_food_family = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_churned_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_color_india_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_couponfujairah_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_cutomersexpiredcouponall3_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_cutomersexpiredcouponnew_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_cutomersexpiredcoupon_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_diwali_big_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_dog_food_family = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_dsf10_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_dubaistore_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_dubaistore_basket_new = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_electronics_1118_section = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_expiry_week_4thfeb_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_expiry_week_5thfeb_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_feb_12_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_feb_18_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_feb_19_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_feb_25_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_feb_26_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_jan_02_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_jan_03_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_jan_04_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_location2410_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_location2410_section = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_locations_0311_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_locations_1110_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_locations_1117_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_locations_1121_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_locations_1124_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_locations_1201_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_locations_1205_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_locations_1215_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_locations_1215_new_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_locations_1222_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_locations_3110_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_locations_amsaf_03_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_locations_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_locations_test_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_location_1207_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_location_2409_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_location_2508_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_location_2520_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_location_all_low_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_location_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_location_big_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_lowetxralow_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_mar_4_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_mar_4_mar_11_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_multichannel_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_new_0921 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_organic_sub_family = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_physical = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_preferredstores_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_preferredstore_16_april_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_preferredstore_1st_april_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_preferredstore_1stapril_churned_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_preferredstore_26march_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_preferredstore_2ndweek_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_preferredstore_35_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_preferredstore_36_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_preferredstore_feb18_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_preferredstore_section = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_promosensitive1104_section = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_promosensitive1104_section_new = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_promosensitive1114_section = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_promosensitive_family = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_promosensitive_section = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_s22_t2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_sharjah_02_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_sow_10_20_40_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_t15 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_t15_coupon_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_t16_coupon_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_t16_coupon_section = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_t2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_t2_1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_t3 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_t3_big = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_t6 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_testingcoupon_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_list_underreached_basket = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_sharpie = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_coupons_stabilo = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_frequency = ColumnSpec(
        data_type='binary',
        description=''
    )

    sb_lifecycle = ColumnSpec(
        data_type='string',
        description=''
    )

    sb_recent_categories = ColumnSpec(
        data_type='binary',
        description=''
    )

    segment = ColumnSpec(
        data_type='string',
        description=''
    )

    share_app_country = ColumnSpec(
        data_type='string',
        description=''
    )

    share_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    share_enrollment = ColumnSpec(
        data_type='boolean',
        description=''
    )

    share_id = ColumnSpec(
        data_type='bigint',
        description=''
    )

    share_member_id = ColumnSpec(
        data_type='binary',
        description=''
    )

    share_points_braze = ColumnSpec(
        data_type='binary',
        description=''
    )

    share_points_braze_2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    share_pseudo_id = ColumnSpec(
        data_type='string',
        description=''
    )

    dat_registration_share = ColumnSpec(
        data_type='timestamp',
        description=''
    )

    share_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    share_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    sharjah_registration_date = ColumnSpec(
        data_type='string',
        description=''
    )

    shiseido_email_subscribe = ColumnSpec(
        data_type='boolean',
        description=''
    )

    shiseido_unsubscribe_date = ColumnSpec(
        data_type='timestamp',
        description=''
    )

    shiseido_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    shishiedo_registration_date = ColumnSpec(
        data_type='timestamp',
        description=''
    )

    skidubai_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    skidubai_registration_date = ColumnSpec(
        data_type='timestamp',
        description=''
    )

    skidubai_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    skidubai_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    skyfii_last_activity_date = ColumnSpec(
        data_type='string',
        description=''
    )

    skyfii_registered_from = ColumnSpec(
        data_type='string',
        description=''
    )

    skyfii_registration_date = ColumnSpec(
        data_type='string',
        description=''
    )

    smiles_activity_count = ColumnSpec(
        data_type='string',
        description=''
    )

    smiles_last_activity_date = ColumnSpec(
        data_type='string',
        description=''
    )

    snowoman_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    snowoman_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    snowoman_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    sprint_23_reco_test_18 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_23_reco_test_19_female = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_23_reco_test_19_male = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_23_reco_test_20_female = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_23_reco_test_20_male = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_23_reco_test_21_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_23_reco_test_22_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_23_reco_test_24_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_23_reco_test_25_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_23_reco_test_26_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_23_reco_test_28_new = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_23_reco_test_28_new2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_24_reco_test_10_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_24_reco_test_11_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_24_reco_test_12_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_24_reco_test_16_arab = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_24_reco_test_16_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_24_reco_test_16_indian = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_24_reco_test_16_indians = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_24_reco_test_16_philipino = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_24_reco_test_16_philipinos = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_24_reco_test_16_western = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_24_reco_test_16_westerners = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_24_reco_test_17_female = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_24_reco_test_17_male = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_24_reco_test_19_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_24_reco_test_1_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_24_reco_test_20_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_24_reco_test_21_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_24_reco_test_23_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_24_reco_test_3_female = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_24_reco_test_3_male = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_24_reco_test_4_female = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_24_reco_test_4_male = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_24_reco_test_7_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_24_reco_test_8_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_25_reco_test_10a_acc = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_25_reco_test_10a_food = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_25_reco_test_10b_acc = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_25_reco_test_10b_acc_new = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_25_reco_test_10b_food = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_25_reco_test_10b_food_new = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_25_reco_test_11_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_25_reco_test_11_default_new = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_25_reco_test_11_food_prep = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_25_reco_test_11_toast_grill = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_25_reco_test_12_food = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_25_reco_test_12_outdoor_fur = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_25_reco_test_12_outdoor_grill = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_25_reco_test_12_outdoor_others = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_25_reco_test_13_food = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_25_reco_test_13_outdoor_fur = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_25_reco_test_13_outdoor_grill = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_25_reco_test_13_outdoor_others = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_25_reco_test_1601_lights_new_top = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_25_reco_test_1601_sweets_new_top = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_25_reco_test_16_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_25_reco_test_1701_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_25_reco_test_17_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_25_reco_test_18_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_25_reco_test_20_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_25_reco_test_20_default_new = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_25_reco_test_2101_acc = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_25_reco_test_2101_acc_new = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_25_reco_test_2101_food = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_25_reco_test_2101_food_new = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_25_reco_test_2102_acc = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_25_reco_test_2102_food = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_25_reco_test_24_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_25_reco_test_25_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_25_reco_test_26_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_25_reco_test_3_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_25_reco_test_3_default_new = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_25_reco_test_4_female = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_25_reco_test_4_female_new = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_25_reco_test_4_male = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_25_reco_test_4_male_new = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_25_reco_test_6_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_25_reco_test_7_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_25_reco_test_9b_acc = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_25_reco_test_9b_acc_new = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_25_reco_test_9b_food = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_25_reco_test_9b_food_new = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_25_reco_test_9_acc = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_25_reco_test_9_acc_new = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_25_reco_test_9_food = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_25_reco_test_9_food_new = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_26_reco_test_1101_snacks = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_26_reco_test_11_snacks = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_26_reco_test_1701_acc = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_26_reco_test_1701_food = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_26_reco_test_17_acc = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_26_reco_test_17_food = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_26_reco_test_1901_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_26_reco_test_19_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_26_reco_test_1_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_26_reco_test_20_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_26_reco_test_24_projectors = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_26_reco_test_24_speakers = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_26_reco_test_24_tv = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_26_reco_test_25_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_26_reco_test_26_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_26_reco_test_2_female = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_26_reco_test_2_male = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_26_reco_test_3_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_26_reco_test_401_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_26_reco_test_4_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_26_reco_test_5_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_26_reco_test_8_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_27_reco_test_11_acc = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_27_reco_test_11_adultcycle = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_27_reco_test_11_kidscycle = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_27_reco_test_23_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_27_reco_test_24_female = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_27_reco_test_24_male = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_27_reco_test_25_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_27_reco_test_25_default2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_27_reco_test_27_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_27_reco_test_28_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_27_reco_test_29_acc = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_27_reco_test_29_acc2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_27_reco_test_29_adultcycle2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_27_reco_test_29_adultcycle3 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_27_reco_test_30_acc = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_27_reco_test_30_acc_new = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_27_reco_test_30_food = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_27_reco_test_30_food_new = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_27_reco_test_31_acc = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_27_reco_test_31_food = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_27_reco_test_33_default_added = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_27_reco_test_3_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_27_reco_test_4_female = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_27_reco_test_4_female_new = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_27_reco_test_4_male = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_27_reco_test_4_male_new = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_27_reco_test_5_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_27_reco_test_6_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_27_reco_test_7_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_28_reco_test_10_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_28_reco_test_15_furniture = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_28_reco_test_15_grill = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_28_reco_test_1901_default2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_28_reco_test_1901_default_tree = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_28_reco_test_19_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_28_reco_test_19_default2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_28_reco_test_19_default3 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_28_reco_test_19_default4 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_28_reco_test_20_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_28_reco_test_20_default1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_28_reco_test_20_default_elec = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_28_reco_test_21_default1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_28_reco_test_21_default_elec_big = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_28_reco_test_22_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_28_reco_test_22_kitchen = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_28_reco_test_24_furniture = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_28_reco_test_26_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_28_reco_test_27_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_28_reco_test_3_female = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_28_reco_test_3_male = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_28_reco_test_4_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_28_reco_test_6_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_28_reco_test_7_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_28_reco_test_8_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_29_reco_test_10_proj = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_29_reco_test_10_speaker = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_29_reco_test_10_tv = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_29_reco_test_11_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_29_reco_test_16_default1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_29_reco_test_17_default1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_29_reco_test_3_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_29_reco_test_4_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_29_reco_test_4_default1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_29_reco_test_4_default2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_29_reco_test_4_default3 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_29_reco_test_4_default4 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_29_reco_test_5_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_29_reco_test_6_default2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_29_reco_test_9_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_29_reco_test_9_default1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_30_reco_test_02_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_30_reco_test_02_default1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_30_reco_test_02_delic = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_30_reco_test_07_hhh = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_30_reco_test_07_hhh1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_30_reco_test_07_lhh = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_30_reco_test_07_lhh1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_30_reco_test_12_acc2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_30_reco_test_12_food = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_30_reco_test_13_acc = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_30_reco_test_13_food = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_30_reco_test_1801_men_grooming = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_30_reco_test_1801_men_skincare = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_30_reco_test_18_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_30_reco_test_18_default1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_30_reco_test_18_female_hair = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_30_reco_test_18_female_skin = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_30_reco_test_19_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_30_reco_test_201_av = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_30_reco_test_201_kitchen = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_30_reco_test_20_av = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_30_reco_test_20_kitchen = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_30_reco_test_20_kitchen_group1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_30_reco_test_2201_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_30_reco_test_22_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_30_reco_test_5_default1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_30_reco_test_5_food_beverages = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_30_reco_test_701_hhh = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_30_reco_test_701_lhh = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_31_reco_test_12_oral_care = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_31_reco_test_1401_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_31_reco_test_14_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_31_reco_test_1602_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_31_reco_test_16_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_31_reco_test_1701_hhh = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_31_reco_test_1701_lhh = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_31_reco_test_17_hhh = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_31_reco_test_17_lhh = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_31_reco_test_18_default2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_31_reco_test_18_default3 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_31_reco_test_401_acc2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_31_reco_test_401_acc3 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_31_reco_test_401_products2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_31_reco_test_401_products_big = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_31_reco_test_401_products_big1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_31_reco_test_401_products_small = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_31_reco_test_401_products_small1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_31_reco_test_4_acc2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_31_reco_test_4_products2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_31_reco_test_4_products_big = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_31_reco_test_4_products_small = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_31_reco_test_5_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_31_reco_test_702_audio_speakers = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_31_reco_test_702_tvs = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_31_reco_test_7_audio_speakers = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_31_reco_test_7_audio_speakers_new1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_31_reco_test_7_tvs = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_31_reco_test_7_tvs_new = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_31_reco_test_8_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_32_reco_test_1001_section_1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_32_reco_test_1001_section_2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_32_reco_test_10_section1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_32_reco_test_10_section2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_32_reco_test_10_section__1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_32_reco_test_18_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_32_reco_test_18_garden_1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_32_reco_test_19_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_32_reco_test_19_default_new = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_32_reco_test_20_female = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_32_reco_test_20_male = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_32_reco_test_21_female = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_32_reco_test_21_male = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_32_reco_test_21_male_new = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_32_reco_test_24_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_32_reco_test_24_default_new = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_32_reco_test_25_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_32_reco_test_26_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_32_reco_test_6_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_32_reco_test_7_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_33_reco_test_04_female = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_33_reco_test_04_male = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_33_reco_test_08_ebars = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_33_reco_test_08_edrink = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_33_reco_test_11_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_33_reco_test_12_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_33_reco_test_12_default_new = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_33_reco_test_12_other_meat = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_33_reco_test_13_storage = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_33_reco_test_14_storage = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_33_reco_test_14_storage_1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_33_reco_test_18_defaults = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_33_reco_test_19_default_all = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_33_reco_test_19_default_all_1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_33_reco_test_20_default_all_30_1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_33_reco_test_24_default_camp = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_33_reco_test_24_default_camp_new = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_33_reco_test_24_default_new = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_33_reco_test_25_defaults = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_33_reco_test_25_defaults_new = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_33_reco_test_25_defaults_new3 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_33_reco_test_25_electronics = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_33_reco_test_25_recommendations = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_33_reco_test_25_regular = ColumnSpec(
        data_type='string',
        description=''
    )

    sprint_33_reco_test_25_thursday = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_33_reco_test_25_thursday_new = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_33_reco_test_27_chicken = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_33_reco_test_27_chicken_lovers = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_33_reco_test_28_chicken = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_33_reco_test_29_pizza = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_33_reco_test_30_valentine = ColumnSpec(
        data_type='string',
        description=''
    )

    sprint_33_reco_test_31_hsh = ColumnSpec(
        data_type='string',
        description=''
    )

    sprint_33_reco_test_6_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_33_reco_test_7_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_33_test_25_trigger = ColumnSpec(
        data_type='string',
        description=''
    )

    sprint_34_reco_test_11_defaults = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_34_reco_test_12_default1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_34_reco_test_12_defaults = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_34_reco_test_13_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_34_reco_test_14_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_34_reco_test_16_default_all_1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_34_reco_test_16_lebanese = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_34_reco_test_16_lebanese1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_34_reco_test_16_lebanese2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_34_reco_test_1901_dph_churned = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_34_reco_test_1901_dph_grocery = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_34_reco_test_19_defaults = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_34_reco_test_19_dph = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_34_reco_test_19_dph_2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_34_reco_test_19_grocery = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_34_reco_test_19_grocery_2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_34_reco_test_20_defaults = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_34_reco_test_20_dph = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_34_reco_test_20_dph1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_34_reco_test_20_dph_2_bf = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_34_reco_test_20_grocery = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_34_reco_test_20_grocery1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_34_reco_test_20_grocery_2_bf = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_34_reco_test_23_defaults = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_34_reco_test_23_veg = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_34_reco_test_24_defaults = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_34_reco_test_26_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_34_reco_test_27_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_34_reco_test_28_recommendations = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_34_reco_test_2_female = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_34_reco_test_2_male = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_34_reco_test_4_phili = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_34_reco_test_7_baby = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_34_reco_test_8_baby = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_34_test_10_coupons = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_35_reco_test_10_default2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_35_reco_test_19_defaults = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_35_reco_test_27_defaults = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_35_reco_test_29_coffee_1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_35_reco_test_29_defaults = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_35_reco_test_29_defaults_new = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_35_reco_test_2_egypt = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_35_reco_test_33_default2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_35_reco_test_34_default2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_35_reco_test_7_skincare = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_35_reco_test_7_skin_3 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_35_reco_test_7_skin_5 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_35_reco_test_7_skin_6 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_35_reco_test_7_skin_7 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_35_reco_test_9_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_35_reco_test_9_default2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_35_reco_test_9_org_1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_35_reco_test_9_org_2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_35_reco_test_9_org_3 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_36_reco_test_12_electronics = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_36_reco_test_12_fresh_veg = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_36_reco_test_13_electronics = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_36_reco_test_13_fresh_veg = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_36_reco_test_15_defaults = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_36_reco_test_20_grocery_1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_36_reco_test_20_grocery_2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_36_reco_test_20_home_1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_36_reco_test_20_personalcare_1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_36_reco_test_31_default2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_36_reco_test_31_default2_new = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_36_reco_test_32_default2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_36_reco_test_4_defaults = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_36_reco_test_4_defaults_1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_36_reco_test_5_defaults_1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_36_reco_test_8_baby_1_1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_36_reco_test_8_baby_1_2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_36_reco_test_8_baby_food = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_36_reco_test_8_baby_other = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_36_reco_test_9_defaults = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_36_reco_test_9_org_1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_36_reco_test_9_org_2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_37_reco_test_10_defaults = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_37_reco_test_18_defaults = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_37_reco_test_26_default2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_37_reco_test_27_default2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_37_reco_test_29_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_37_reco_test_9_defaults = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_38_reco_test_14_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_38_reco_test_19_default2temp = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_38_reco_test_20_default2temp = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_38_reco_test_8_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_38_reco_test_8_default2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_39_reco_test_10_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_39_reco_test_10_default_new = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_39_reco_test_12_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_39_reco_test_15_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_39_reco_test_16_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_39_reco_test_27_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_39_reco_test_27_default1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_39_reco_test_28_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_39_reco_test_28_default1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_39_reco_test_30_fruits = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_39_reco_test_30_fruits1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_39_reco_test_30_fruits2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_39_reco_test_30_vegetables = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_39_reco_test_30_vegetables1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_39_reco_test_30_vegetables2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_39_reco_test_31_fruits = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_39_reco_test_31_fruits1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_39_reco_test_31_fruits2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_39_reco_test_31_vegetables = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_39_reco_test_31_vegetables1 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_39_reco_test_31_vegetables2 = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_39_reco_test_34_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_39_reco_test_3_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_39_reco_test_4_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_39_reco_test_8_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_39_reco_test_8_default_new = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_39_reco_test_9_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    sprint_reco_recoattributes_default = ColumnSpec(
        data_type='binary',
        description=''
    )

    storename_ar = ColumnSpec(
        data_type='string',
        description=''
    )

    storename_en = ColumnSpec(
        data_type='string',
        description=''
    )

    subject_line_one = ColumnSpec(
        data_type='string',
        description=''
    )

    subject_line_two = ColumnSpec(
        data_type='string',
        description=''
    )

    subscription_id = ColumnSpec(
        data_type='string',
        description=''
    )

    substitution_url = ColumnSpec(
        data_type='string',
        description=''
    )

    des_summer_activity = ColumnSpec(
        data_type='binary',
        description=''
    )

    sw_completion_status = ColumnSpec(
        data_type='string',
        description=''
    )

    sw_grocery_last_bit_date = ColumnSpec(
        data_type='string',
        description=''
    )

    sw_grocery_status = ColumnSpec(
        data_type='string',
        description=''
    )

    sw_hotel_last_bit_date = ColumnSpec(
        data_type='string',
        description=''
    )

    sw_hotel_status = ColumnSpec(
        data_type='string',
        description=''
    )

    sw_lifestyle_last_bit_date = ColumnSpec(
        data_type='string',
        description=''
    )

    sw_lifestyle_status = ColumnSpec(
        data_type='string',
        description=''
    )

    sw_l_and_e_last_bit_date = ColumnSpec(
        data_type='string',
        description=''
    )

    sw_l_and_e_status = ColumnSpec(
        data_type='string',
        description=''
    )

    sw_mall_last_bit_date = ColumnSpec(
        data_type='string',
        description=''
    )

    sw_mall_status = ColumnSpec(
        data_type='string',
        description=''
    )

    sw_segment_code = ColumnSpec(
        data_type='string',
        description=''
    )

    template_final_name = ColumnSpec(
        data_type='string',
        description=''
    )

    test_attribute = ColumnSpec(
        data_type='binary',
        description=''
    )

    that_email_subscribe = ColumnSpec(
        data_type='boolean',
        description=''
    )

    that_registration_date = ColumnSpec(
        data_type='timestamp',
        description=''
    )

    that_unsubscribe_date = ColumnSpec(
        data_type='timestamp',
        description=''
    )

    that_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    threshold = ColumnSpec(
        data_type='binary',
        description=''
    )

    time = ColumnSpec(
        data_type='string',
        description=''
    )

    time_zone = ColumnSpec(
        data_type='string',
        description=''
    )

    nam_title = ColumnSpec(
        data_type='string',
        description=''
    )

    top_banner_image_url = ColumnSpec(
        data_type='string',
        description=''
    )

    top_banner_url = ColumnSpec(
        data_type='string',
        description=''
    )

    qty_accrued_total = ColumnSpec(
        data_type='string',
        description=''
    )

    total_order_value = ColumnSpec(
        data_type='string',
        description=''
    )

    qty_redeemed_total = ColumnSpec(
        data_type='string',
        description=''
    )

    total_revenue = ColumnSpec(
        data_type='decimal(11,3)',
        description=''
    )

    trigger_antichurn_trigger = ColumnSpec(
        data_type='string',
        description=''
    )

    trigger_coupon_use_trigger = ColumnSpec(
        data_type='string',
        description=''
    )

    trigger_onboardingyourcrf_trigger = ColumnSpec(
        data_type='string',
        description=''
    )

    trigger_onboarding_coupon = ColumnSpec(
        data_type='binary',
        description=''
    )

    type = ColumnSpec(
        data_type='string',
        description=''
    )

    ucg_group = ColumnSpec(
        data_type='binary',
        description=''
    )

    ucg_utg_tl_flag = ColumnSpec(
        data_type='binary',
        description=''
    )

    uninstalled_at = ColumnSpec(
        data_type='timestamp',
        description=''
    )

    unique_link = ColumnSpec(
        data_type='string',
        description=''
    )

    unique_link_test = ColumnSpec(
        data_type='string',
        description=''
    )

    uplift_campaigns = ColumnSpec(
        data_type='binary',
        description=''
    )

    user_aliases = ColumnSpec(
        data_type='binary',
        description=''
    )

    user_privacy_blacklist_status = ColumnSpec(
        data_type='boolean',
        description=''
    )

    utg_group = ColumnSpec(
        data_type='binary',
        description=''
    )

    vox_email_subscribe = ColumnSpec(
        data_type='string',
        description=''
    )

    vox_registration_date = ColumnSpec(
        data_type='timestamp',
        description=''
    )

    vox_unsubscribe_date = ColumnSpec(
        data_type='string',
        description=''
    )

    vox_unsubscribe_reason = ColumnSpec(
        data_type='string',
        description=''
    )

    vt_store_name = ColumnSpec(
        data_type='binary',
        description=''
    )

    whatsapp_city = ColumnSpec(
        data_type='string',
        description=''
    )

    whatsapp_country = ColumnSpec(
        data_type='string',
        description=''
    )

    whatsapp_name = ColumnSpec(
        data_type='string',
        description=''
    )

    whatsapp_preferred_language = ColumnSpec(
        data_type='string',
        description=''
    )

    whatsapp_subscribe_status = ColumnSpec(
        data_type='string',
        description=''
    )

    your_attribute = ColumnSpec(
        data_type='string',
        description=''
    )

    dat_batch = ColumnSpec(
        data_type='date',
        description=''
    )

    ide_telephone = ColumnSpec(
        data_type='string',
        description=''
    )
