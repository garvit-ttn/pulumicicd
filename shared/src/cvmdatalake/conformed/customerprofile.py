from cvmdatalake import ColumnSpec, TableSpec
from cvmdatalake.synthetic import FakerMapping


class CustomerProfile(TableSpec):

    @classmethod
    def table_description(cls):
        return "Customer 360 Profiles"

        # Demographics

    bu = ColumnSpec(
        is_partition=True,
        data_type='string',
        description='',
    )

    # Demographics
    idi_counterparty = ColumnSpec(
        data_type='string',
        description='',
        custom_attributes=[FakerMapping(method='uuid4')]
    )

    cod_sor_counterparty = ColumnSpec(
        data_type='string',
        description='',
        custom_attributes=[FakerMapping(method='iana_id')]
    )

    cde_country_residence = ColumnSpec(
        data_type='string',
        description='',
        custom_attributes=[FakerMapping(method='country')]
    )

    ind_person_gender = ColumnSpec(
        data_type='string',
        description='',
        custom_attributes=[FakerMapping(method='passport_gender')]
    )

    adr_visit_address_string = ColumnSpec(
        data_type='string',
        description=''
    )

    adr_visit_local_specific = ColumnSpec(
        data_type='string',
        description=''
    )

    adr_visit_cityname = ColumnSpec(
        data_type='string',
        description=''
    )

    des_age = ColumnSpec(
        data_type='bigint',
        description=''
    )

    counterparty_role = ColumnSpec(
        data_type='string',
        description=''
    )

    des_language = ColumnSpec(
        data_type='string',
        description=''
    )

    des_nationality = ColumnSpec(
        data_type='string',
        description=''
    )

    # NBO outputs
    nbo1 = ColumnSpec(
        data_type='string',
        description=''
    )

    nbo2 = ColumnSpec(
        data_type='string',
        description=''
    )

    nbo3 = ColumnSpec(
        data_type='string',
        description=''
    )

    nbo4 = ColumnSpec(
        data_type='string',
        description=''
    )

    nbo5 = ColumnSpec(
        data_type='string',
        description=''
    )

    # CLV outputs

    max_date = ColumnSpec(
        data_type='date',
        description='',
        custom_attributes=[FakerMapping(method='date')]
    )

    min_date = ColumnSpec(
        data_type='date',
        description='',
        custom_attributes=[FakerMapping(method='date')]
    )

    frequency = ColumnSpec(
        data_type='int',
        description=''
    )

    monetary = ColumnSpec(
        data_type='float',
        description=''
    )

    # recency = ColumnSpec(
    #     data_type='int',
    #     description=''
    # )

    customer_age = ColumnSpec(
        data_type='bigint',
        description=''
    )

    expected_purchases_3m = ColumnSpec(
        data_type='float',
        description=''
    )

    clv_3m = ColumnSpec(
        data_type='float',
        description=''
    )

    p_alive = ColumnSpec(
        data_type='float',
        description=''
    )

    # RFM outputs
    recency_proposition_level_3 = ColumnSpec(
        data_type='int',
        description=''
    )

    # idi_proposition_level03 = ColumnSpec(
    #     data_type='string',
    #     description=''
    # )

    frequency_proposition_level_3 = ColumnSpec(
        data_type='int',
        description=''
    )

    monetary_proposition_level_3 = ColumnSpec(
        data_type='float',
        description=''
    )

    r_rank_norm_proposition_level_3 = ColumnSpec(
        data_type='string',
        description=''
    )

    f_rank_norm_proposition_level_3 = ColumnSpec(
        data_type='string',
        description=''
    )

    m_rank_norm_proposition_level_3 = ColumnSpec(
        data_type='string',
        description=''
    )

    customer_segment_proposition_level_3 = ColumnSpec(
        data_type='string',
        description=''
    )

    # KPIs
    sow = ColumnSpec(
        data_type='int',
        description=''
    )

    sow_moe = ColumnSpec(
        data_type='int',
        description=''
    )

    sow_cc_d = ColumnSpec(
        data_type='int',
        description=''
    )

    pta = ColumnSpec(
        data_type='string',
        description=''
    )

    sta = ColumnSpec(
        data_type='string',
        description=''
    )

    turnover_6m = ColumnSpec(
        data_type='int',
        description=''
    )

    turnover_3m = ColumnSpec(
        data_type='int',
        description=''
    )

    turnover_12m = ColumnSpec(
        data_type='int',
        description=''
    )

    turnover_3m_ni = ColumnSpec(
        data_type='int',
        description=''
    )

    turnover_3m_moe = ColumnSpec(
        data_type='int',
        description=''
    )

    engagement_status = ColumnSpec(
        data_type='string',
        description=''
    )

    bu_turnover = ColumnSpec(
        data_type='string',
        description=''
    )

    ni_locations = ColumnSpec(
        data_type='string',
        description=''
    )

    number_of_visits_12m = ColumnSpec(
        data_type='int',
        description=''
    )

    # Braze Attributes
    dat_enrollment = ColumnSpec(
        data_type='date',
        description=''
    )

    nam_enrollment_sponsor = ColumnSpec(
        data_type='string',
        description=''
    )

    dat_birth = ColumnSpec(
        data_type='date',
        description=''
    )

    dat_registration_share = ColumnSpec(
        data_type='date',
        description=''
    )

    nam_title = ColumnSpec(
        data_type='string',
        description=''
    )

    qty_points_balance = ColumnSpec(
        data_type='float',
        description=''
    )

    dat_last_accrual = ColumnSpec(
        data_type='date',
        description=''
    )

    dat_last_activity = ColumnSpec(
        data_type='date',
        description=''
    )

    dat_last_redemption = ColumnSpec(
        data_type='date',
        description=''
    )

    qty_accrued_total = ColumnSpec(
        data_type='float',
        description=''
    )

    qty_redeemed_total = ColumnSpec(
        data_type='float',
        description=''
    )

    cde_country_of_residence = ColumnSpec(
        data_type='string',
        description=''
    )

    des_membership_stage = ColumnSpec(
        data_type='string',
        description=''
    )

    dat_last_login = ColumnSpec(
        data_type='date',
        description=''
    )

    ind_push_enabled = ColumnSpec(
        data_type='string',
        description=''
    )

    des_device_model = ColumnSpec(
        data_type='string',
        description=''
    )

    des_device_os = ColumnSpec(
        data_type='string',
        description=''
    )

    des_device_carrier = ColumnSpec(
        data_type='string',
        description=''
    )

    qty_sessions = ColumnSpec(
        data_type='string',
        description=''
    )

    dat_first_session = ColumnSpec(
        data_type='date',
        description=''
    )

    dat_last_session = ColumnSpec(
        data_type='date',
        description=''
    )

    nam_country = ColumnSpec(
        data_type='string',
        description=''
    )

    sw_completion_status = ColumnSpec(
        data_type='string',
        description=''
    )

    sw_grocery_last_bit_date = ColumnSpec(
        data_type='date',
        description=''
    )

    sw_grocery_status = ColumnSpec(
        data_type='string',
        description=''
    )

    sw_hotel_last_bit_date = ColumnSpec(
        data_type='date',
        description=''
    )

    sw_hotel_status = ColumnSpec(
        data_type='string',
        description=''
    )

    sw_l_and_e_last_bit_date = ColumnSpec(
        data_type='date',
        description=''
    )

    sw_l_and_e_status = ColumnSpec(
        data_type='string',
        description=''
    )

    sw_lifestyle_last_bit_date = ColumnSpec(
        data_type='date',
        description=''
    )

    sw_lifestyle_status = ColumnSpec(
        data_type='string',
        description=''
    )

    sw_mall_last_bit_date = ColumnSpec(
        data_type='date',
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

    fab_cobrand_product = ColumnSpec(
        data_type='string',
        description=''
    )

    favfruit = ColumnSpec(
        data_type='string',
        description=''
    )

    share_active_30_days = ColumnSpec(
        data_type='string',
        description=''
    )

    share_active_60_days = ColumnSpec(
        data_type='string',
        description=''
    )

    share_active_90_days = ColumnSpec(
        data_type='string',
        description=''
    )

    share_active_180_days = ColumnSpec(
        data_type='string',
        description=''
    )

    ind_coupon_generated = ColumnSpec(
        data_type='string',
        description=''
    )

    dat_first_login = ColumnSpec(
        data_type='date',
        description=''
    )

    dat_first_login_share = ColumnSpec(
        data_type='date',
        description=''
    )

    gulf_news = ColumnSpec(
        data_type='string',
        description=''
    )

    isd_code = ColumnSpec(
        data_type='string',
        description=''
    )

    cde_mal = ColumnSpec(
        data_type='string',
        description=''
    )

    offline_first_maf_contact = ColumnSpec(
        data_type='date',
        description=''
    )

    qty_points_won_spin_wheel = ColumnSpec(
        data_type='float',
        description=''
    )

    des_language_preferred = ColumnSpec(
        data_type='string',
        description=''
    )

    registered_from = ColumnSpec(
        data_type='string',
        description=''
    )

    dat_registration = ColumnSpec(
        data_type='date',
        description=''
    )

    des_share_world_segment = ColumnSpec(
        data_type='string',
        description=''
    )

    des_summer_activity = ColumnSpec(
        data_type='string',
        description=''
    )

    ide_cc_mall_code = ColumnSpec(
        data_type='string',
        description=''
    )

    des_interests = ColumnSpec(
        data_type='string',
        description=''
    )

    ind_logged_in = ColumnSpec(
        data_type='string',
        description=''
    )

    qty_payment_card = ColumnSpec(
        data_type='float',
        description=''
    )

    lifestyle_total_txn_last_last_6_months = ColumnSpec(
        data_type='float',
        description=''
    )

    lifestyle_avg_spend_last_6_months = ColumnSpec(
        data_type='float',
        description=''
    )

    lifestyle_avg_spend_last_3_months = ColumnSpec(
        data_type='float',
        description=''
    )

    lifestyle_avg_spend_last_2_months = ColumnSpec(
        data_type='float',
        description=''
    )

    lifestyle_avg_spend_last_1_month = ColumnSpec(
        data_type='float',
        description=''
    )

    lifestyle_luxury_customer_flag = ColumnSpec(
        data_type='string',
        description=''
    )

    nam_brand = ColumnSpec(
        data_type='string',
        description=''
    )

    lifestyle_furniture_category_spend_last_3_months = ColumnSpec(
        data_type='float',
        description=''
    )

    lifestyle_furniture_category_spend_last_1_month = ColumnSpec(
        data_type='float',
        description=''
    )

    lifestyle_furniture_category_spend_last_7_days = ColumnSpec(
        data_type='float',
        description=''
    )

    lifestyle_decor_category_spend_last_3_months = ColumnSpec(
        data_type='float',
        description=''
    )

    lifestyle_decor_category_spend_last_1_month = ColumnSpec(
        data_type='float',
        description=''
    )

    lifestyle_decor_category_spend_last_7_days = ColumnSpec(
        data_type='float',
        description=''
    )

    lifestyle_kids_category_spend_last_3_months = ColumnSpec(
        data_type='float',
        description=''
    )

    lifestyle_kids_category_spend_last_1_month = ColumnSpec(
        data_type='float',
        description=''
    )

    lifestyle_kids_category_spend_last_7_days = ColumnSpec(
        data_type='float',
        description=''
    )

    des_language_customer = ColumnSpec(
        data_type='string',
        description=''
    )

    vox_avg_txn_value_for_12_months = ColumnSpec(
        data_type='float',
        description=''
    )

    vox_total_spend_for_12_months = ColumnSpec(
        data_type='float',
        description=''
    )

    vox_arabic_movie_count = ColumnSpec(
        data_type='bigint',
        description=''
    )

    vox_hindi_movie_count = ColumnSpec(
        data_type='bigint',
        description=''
    )

    vox_children_movie_count = ColumnSpec(
        data_type='bigint',
        description=''
    )

    vox_cinema_total_spend_last_12_months = ColumnSpec(
        data_type='float',
        description=''
    )

    vox_cinema_total_spend_last_6_months = ColumnSpec(
        data_type='float',
        description=''
    )

    vox_cinema_total_spend_last_3_months = ColumnSpec(
        data_type='float',
        description=''
    )

    maf_cinema_total_txn_last_12_months = ColumnSpec(
        data_type='float',
        description=''
    )

    maf_cinema_total_txn_last_6_months = ColumnSpec(
        data_type='float',
        description=''
    )

    maf_cinema_total_txn_last_3_months = ColumnSpec(
        data_type='float',
        description=''
    )

    vox_total_number_transactions = ColumnSpec(
        data_type='bigint',
        description=''
    )

    vox_family_movie_total_revenue = ColumnSpec(
        data_type='float',
        description=''
    )

    vox_family_movie_total_txn = ColumnSpec(
        data_type='bigint',
        description=''
    )

    vox_scifi_movie_total_txn = ColumnSpec(
        data_type='bigint',
        description=''
    )

    vox_arabic_movie_total_txn = ColumnSpec(
        data_type='bigint',
        description=''
    )

    vox_kids_movie_total_txn = ColumnSpec(
        data_type='bigint',
        description=''
    )

    vox_preferred_mall = ColumnSpec(
        data_type='string',
        description=''
    )

    vox_luxury_concept_flag = ColumnSpec(
        data_type='string',
        description=''
    )

    vox_most_recent_movie_watched = ColumnSpec(
        data_type='string',
        description=''
    )

    vox_number_movies_last_1_month = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lne_avg_txn_for_12_months_246 = ColumnSpec(
        data_type='float',
        description=''
    )

    lne_avg_txn_for_12_months_267 = ColumnSpec(
        data_type='float',
        description=''
    )

    lne_no_txn_for_12_months = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lne_total_spend_for_12_months = ColumnSpec(
        data_type='float',
        description=''
    )

    lne_total_transactions_last_6_months = ColumnSpec(
        data_type='float',
        description=''
    )

    lne_total_transactions_last_3_months = ColumnSpec(
        data_type='float',
        description=''
    )

    lne_customer_luxury_flag = ColumnSpec(
        data_type='string',
        description=''
    )

    lec_total_spend_for_12_months = ColumnSpec(
        data_type='float',
        description=''
    )

    lec_mp_total_txn_last_6_months = ColumnSpec(
        data_type='float',
        description=''
    )

    lec_mp_total_txn_last_3_months = ColumnSpec(
        data_type='float',
        description=''
    )

    lec_mp_avg_txn_value_for_12_months = ColumnSpec(
        data_type='float',
        description=''
    )

    ski_total_txn_last_6_months = ColumnSpec(
        data_type='float',
        description=''
    )

    ski_total_txn_last_3_months = ColumnSpec(
        data_type='float',
        description=''
    )

    ski_avg_txn_value_last_12_months = ColumnSpec(
        data_type='float',
        description=''
    )

    app_id = ColumnSpec(
        data_type='string',
        description=''
    )

    lifecycle_stage_als = ColumnSpec(
        data_type='string',
        description=''
    )

    lifecycle_stage_carrefour = ColumnSpec(
        data_type='string',
        description=''
    )

    lifecycle_stage_ccd = ColumnSpec(
        data_type='string',
        description=''
    )

    lifecycle_stage_ccmi = ColumnSpec(
        data_type='string',
        description=''
    )

    lifecycle_stage_cnb = ColumnSpec(
        data_type='string',
        description=''
    )

    lifecycle_stage_crf = ColumnSpec(
        data_type='string',
        description=''
    )

    lifecycle_stage_lec = ColumnSpec(
        data_type='string',
        description=''
    )

    lifecycle_stage_lifestyle = ColumnSpec(
        data_type='string',
        description=''
    )

    lifecycle_stage_lll = ColumnSpec(
        data_type='string',
        description=''
    )

    lifecycle_stage_magicp = ColumnSpec(
        data_type='string',
        description=''
    )

    lifecycle_stage_moe = ColumnSpec(
        data_type='string',
        description=''
    )

    lifecycle_stage_share = ColumnSpec(
        data_type='string',
        description=''
    )

    lifecycle_stage_skidub = ColumnSpec(
        data_type='string',
        description=''
    )

    lifecycle_stage_smbu = ColumnSpec(
        data_type='string',
        description=''
    )

    lifecycle_stage_vox = ColumnSpec(
        data_type='string',
        description=''
    )

    purchase_propensity_als_next_3_months = ColumnSpec(
        data_type='float',
        description=''
    )

    purchase_propensity_carrefour_next_3_months = ColumnSpec(
        data_type='float',
        description=''
    )

    purchase_propensity_ccd_next_3_months = ColumnSpec(
        data_type='float',
        description=''
    )

    purchase_propensity_ccmi_next_3_months = ColumnSpec(
        data_type='float',
        description=''
    )

    purchase_propensity_cnb_next_3_months = ColumnSpec(
        data_type='float',
        description=''
    )

    purchase_propensity_crfnext_3_months = ColumnSpec(
        data_type='float',
        description=''
    )

    purchase_propensity_lec_next_3_months = ColumnSpec(
        data_type='float',
        description=''
    )

    purchase_propensity_lifestyle_next_3_months = ColumnSpec(
        data_type='float',
        description=''
    )

    purchase_propensity_lll_next_3_months = ColumnSpec(
        data_type='float',
        description=''
    )

    purchase_propensity_magicp_next_3_months = ColumnSpec(
        data_type='float',
        description=''
    )

    purchase_propensity_moe_next_3_months = ColumnSpec(
        data_type='float',
        description=''
    )

    purchase_propensity_share_next_3_months = ColumnSpec(
        data_type='float',
        description=''
    )

    purchase_propensity_skidub_next_3_months = ColumnSpec(
        data_type='float',
        description=''
    )

    purchase_propensity_smbu_next_3_months = ColumnSpec(
        data_type='float',
        description=''
    )

    purchase_propensity_vox_next_3_months = ColumnSpec(
        data_type='float',
        description=''
    )

    total_spend_als_last_3_months = ColumnSpec(
        data_type='float',
        description=''
    )

    total_spend_als_last_6_months = ColumnSpec(
        data_type='float',
        description=''
    )

    total_spend_carrefour_last_3_months = ColumnSpec(
        data_type='float',
        description=''
    )

    total_spend_carrefour_last_6_months = ColumnSpec(
        data_type='float',
        description=''
    )

    total_spend_ccd_last_3_months = ColumnSpec(
        data_type='float',
        description=''
    )

    total_spend_ccd_last_6_months = ColumnSpec(
        data_type='float',
        description=''
    )

    total_spend_ccmi_last_3_months = ColumnSpec(
        data_type='float',
        description=''
    )

    total_spend_ccmi_last_6_months = ColumnSpec(
        data_type='float',
        description=''
    )

    total_spend_cnb_last_3_months = ColumnSpec(
        data_type='float',
        description=''
    )

    total_spend_cnb_last_6_months = ColumnSpec(
        data_type='float',
        description=''
    )

    total_spend_crf_last_3_months = ColumnSpec(
        data_type='float',
        description=''
    )

    total_spend_crf_last_6_months = ColumnSpec(
        data_type='float',
        description=''
    )

    total_spend_lec_last_3_months = ColumnSpec(
        data_type='float',
        description=''
    )

    total_spend_lec_last_6_months = ColumnSpec(
        data_type='float',
        description=''
    )

    total_spend_lifestyle_last_3_months = ColumnSpec(
        data_type='float',
        description=''
    )

    total_spend_lifestyle_last_6_months = ColumnSpec(
        data_type='float',
        description=''
    )

    total_spend_lll_last_3_months = ColumnSpec(
        data_type='float',
        description=''
    )

    total_spend_lll_last_6_months = ColumnSpec(
        data_type='float',
        description=''
    )

    total_spend_magicp_last_3_months = ColumnSpec(
        data_type='float',
        description=''
    )

    total_spend_moe_last_12_months = ColumnSpec(
        data_type='float',
        description=''
    )

    total_spend_moe_last_1_day = ColumnSpec(
        data_type='float',
        description=''
    )

    total_spend_moe_last_1_month = ColumnSpec(
        data_type='float',
        description=''
    )

    total_spend_moe_last_3_months = ColumnSpec(
        data_type='float',
        description=''
    )

    total_spend_moe_last_6_months = ColumnSpec(
        data_type='float',
        description=''
    )

    total_spend_moe_last_7_days = ColumnSpec(
        data_type='float',
        description=''
    )

    total_spend_share_last_3_months = ColumnSpec(
        data_type='float',
        description=''
    )

    total_spend_share_last_6_months = ColumnSpec(
        data_type='float',
        description=''
    )

    total_spend_share_last_7_days = ColumnSpec(
        data_type='float',
        description=''
    )

    total_spend_skidub_last_3_months = ColumnSpec(
        data_type='float',
        description=''
    )

    total_spend_skidub_last_6_months = ColumnSpec(
        data_type='float',
        description=''
    )

    total_spend_smbu_last_3_months = ColumnSpec(
        data_type='float',
        description=''
    )

    total_spend_smbu_last_6_months = ColumnSpec(
        data_type='float',
        description=''
    )

    total_spend_vox_last_3_months = ColumnSpec(
        data_type='float',
        description=''
    )

    total_spend_vox_last_6_months = ColumnSpec(
        data_type='float',
        description=''
    )

    total_transactions_share_partner_last_1_day = ColumnSpec(
        data_type='bigint',
        description=''
    )

    total_transactions_share_partner_last_7_days = ColumnSpec(
        data_type='bigint',
        description=''
    )

    total_transactions_share_partner_last_1_month = ColumnSpec(
        data_type='bigint',
        description=''
    )

    total_transactions_share_partner_last_3_months = ColumnSpec(
        data_type='bigint',
        description=''
    )

    total_transactions_share_partner_last_6_months = ColumnSpec(
        data_type='bigint',
        description=''
    )

    total_transactions_share_partner_last_12_months = ColumnSpec(
        data_type='bigint',
        description=''
    )

    total_spend_share_partner_last_1_day = ColumnSpec(
        data_type='float',
        description=''
    )

    total_spend_share_partner_last_7_days = ColumnSpec(
        data_type='float',
        description=''
    )

    total_spend_share_partner_last_1_month = ColumnSpec(
        data_type='float',
        description=''
    )

    total_spend_share_partner_last_3_months = ColumnSpec(
        data_type='float',
        description=''
    )

    total_spend_share_partner_last_6_months = ColumnSpec(
        data_type='float',
        description=''
    )

    total_spend_share_partner_last_12_months = ColumnSpec(
        data_type='float',
        description=''
    )

    partner_first_transaction_date = ColumnSpec(
        data_type='date',
        description=''
    )

    partner_last_transaction_date = ColumnSpec(
        data_type='date',
        description=''
    )

    partner_engagement_status = ColumnSpec(
        data_type='int',
        description=''
    )

    purchase_propensity_luxury = ColumnSpec(
        data_type='float',
        description=''
    )

    purchase_propensity_moe_luxury_next_3_months = ColumnSpec(
        data_type='float',
        description=''
    )

    purchase_propensity_moe_groceries_next_3_months = ColumnSpec(
        data_type='float',
        description=''
    )

    purchase_propensity_moe_fnb_next_3_months = ColumnSpec(
        data_type='float',
        description=''
    )

    purchase_propensity_moe_fashion_accessories_next_3_months = ColumnSpec(
        data_type='float',
        description=''
    )

    purchase_propensity_ccmi_luxury_next_3_months = ColumnSpec(
        data_type='float',
        description=''
    )

    purchase_propensity_ccmi_fashion_accessories_next_3_months = ColumnSpec(
        data_type='float',
        description=''
    )

    purchase_propensity_ccmi_homefurniture_electronics_next_3_months = ColumnSpec(
        data_type='float',
        description=''
    )

    purchase_propensity_ccmi_beauty_next_3_months = ColumnSpec(
        data_type='float',
        description=''
    )

    purchase_propensity_ccmi_fnb_next_3_months = ColumnSpec(
        data_type='float',
        description=''
    )

    nbo6 = ColumnSpec(
        data_type='string',
        description=''
    )

    total_spend_moe_luxury_last_7_days = ColumnSpec(
        data_type='float',
        description=''
    )

    total_spend_moe_luxury_last_1_month = ColumnSpec(
        data_type='float',
        description=''
    )

    total_spend_moe_luxury_last_3_months = ColumnSpec(
        data_type='float',
        description=''
    )

    total_spend_moe_luxury_last_6_months = ColumnSpec(
        data_type='float',
        description=''
    )

    total_spend_moe_luxury_last_12_months = ColumnSpec(
        data_type='float',
        description=''
    )

    total_spend_ccmi_luxury_last_7_days = ColumnSpec(
        data_type='float',
        description=''
    )

    total_spend_ccmi_luxury_last_1_month = ColumnSpec(
        data_type='float',
        description=''
    )

    total_spend_ccmi_luxury_last_3_months = ColumnSpec(
        data_type='float',
        description=''
    )

    total_spend_ccmi_luxury_last_6_months = ColumnSpec(
        data_type='float',
        description=''
    )

    total_spend_ccmi_luxury_last_12_months = ColumnSpec(
        data_type='float',
        description=''
    )

    next_best_share_partners = ColumnSpec(
        data_type='string',
        description=''
    )

    next_best_category_moe = ColumnSpec(
        data_type='string',
        description=''
    )

    next_best_category_ccmi = ColumnSpec(
        data_type='string',
        description=''
    )

    next_best_category_share = ColumnSpec(
        data_type='string',
        description=''
    )

    next_best_category_lifestyle = ColumnSpec(
        data_type='string',
        description=''
    )

    next_best_category_that = ColumnSpec(
        data_type='string',
        description=''
    )

    next_best_category_cnb = ColumnSpec(
        data_type='string',
        description=''
    )

    next_best_category_lll = ColumnSpec(
        data_type='string',
        description=''
    )

    next_best_category_lego = ColumnSpec(
        data_type='string',
        description=''
    )

    next_best_category_als = ColumnSpec(
        data_type='string',
        description='next'
    )

    next_best_category_cb2 = ColumnSpec(
        data_type='string',
        description=''
    )

    next_best_category_anf = ColumnSpec(
        data_type='string',
        description=''
    )

    next_best_category_hco = ColumnSpec(
        data_type='string',
        description=''
    )

    next_best_category_shi = ColumnSpec(
        data_type='string',
        description=''
    )

    next_best_category_pf = ColumnSpec(
        data_type='string',
        description=''
    )

    next_best_product_lifestyle_by_brand = ColumnSpec(
        data_type='string',
        description=''
    )

    experiences = ColumnSpec(
        data_type='string',
        description=''
    )

    category_group = ColumnSpec(
        data_type='string',
        description=''
    )

    store_category = ColumnSpec(
        data_type='string',
        description=''
    )

    total_transactions_share_partner_fab_last_1_day = ColumnSpec(
        data_type='bigint',
        description=''
    )

    total_transactions_share_partner_fab_last_7_day = ColumnSpec(
        data_type='bigint',
        description=''
    )

    total_transactions_share_partner_fab_last_1_months = ColumnSpec(
        data_type='bigint',
        description=''
    )

    total_transactions_share_partner_fab_last_3_months = ColumnSpec(
        data_type='bigint',
        description=''
    )

    total_transactions_share_partner_fab_last_6_months = ColumnSpec(
        data_type='bigint',
        description=''
    )

    total_transactions_share_partner_fab_last_12_months = ColumnSpec(
        data_type='bigint',
        description=''
    )

    total_spend_share_fab_last_1_day = ColumnSpec(
        data_type='double',
        description=''
    )

    total_spend_share_fab_last_7_day = ColumnSpec(
        data_type='double',
        description=''
    )

    total_spend_share_fab_last_1_month = ColumnSpec(
        data_type='double',
        description=''
    )

    total_spend_share_fab_last_3_month = ColumnSpec(
        data_type='double',
        description=''
    )

    total_spend_share_fab_last_6_month = ColumnSpec(
        data_type='double',
        description=''
    )

    total_spend_share_fab_last_12_month = ColumnSpec(
        data_type='double',
        description=''
    )

    share_partner_fab_first_transaction_date = ColumnSpec(
        data_type='date',
        description=''
    )

    share_partner_fab_last_transaction_date = ColumnSpec(
        data_type='date',
        description=''
    )

    share_partner_fab_engagement_status = ColumnSpec(
        data_type='int',
        description=''
    )

    share_partner_costa_engagement_status = ColumnSpec(
        data_type='int',
        description=''
    )

    share_partner_gn_engagement_status = ColumnSpec(
        data_type='int',
        description=''
    )

    share_partner_etihad_engagement_status = ColumnSpec(
        data_type='int',
        description=''
    )

    share_partner_aljaber_engagement_status = ColumnSpec(
        data_type='int',
        description=''
    )

    share_partner_smiles_engagement_status = ColumnSpec(
        data_type='int',
        description=''
    )

    share_partner_bookingcom_engagement_status = ColumnSpec(
        data_type='int',
        description=''
    )


    st_mall = ColumnSpec(
        data_type='string',
        description=''
    )

    st_brand = ColumnSpec(
        data_type='string',
        description=''
    )


# total_transactions_share_partner_last_1_day
# total_transactions_share_partner_last_7_days
# total_transactions_share_partner_last_1_month
# total_transactions_share_partner_last_3_months
# total_transactions_share_partner_last_6_months
# total_transactions_share_partner_last_12_months
# total_spend_share_partner_last_1_day
# total_spend_share_partner_last_7_days
# total_spend_share_partner_last_1_month
# total_spend_share_partner_last_3_months
# total_spend_share_partner_last_6_months
# total_spend_share_partner_last_12_months
# partner_first_transaction_date
# partner_last_transaction_date
# partner_engagement_status
#
# purchase_propensity_luxury
# purchase_propensity_moe_luxury_next_3_months
# purchase_propensity_moe_groceries_next_3_months
# purchase_propensity_moe_fnb_next_3_months
# purchase_propensity_moe_fashion_accessories_next_3_months
# purchase_propensity_ccmi_luxury_next_3_months
# purchase_propensity_ccmi_fashion_accessories_next_3_months
# purchase_propensity_ccmi_homefurniture_electronics_next_3_months
# purchase_propensity_ccmi_beauty_next_3_months
# purchase_propensity_ccmi_fnb_next_3_months
#
# nbo6
#
# total_spend_moe_luxury_last_7_days
# total_spend_moe_luxury_last_1_month
# total_spend_moe_luxury_last_3_months
# total_spend_moe_luxury_last_6_months
# total_spend_moe_luxury_last_12_months
#
# total_spend_ccmi_luxury_last_7_days
# total_spend_ccmi_luxury_last_1_month
# total_spend_ccmi_luxury_last_3_months
# total_spend_ccmi_luxury_last_6_months
# total_spend_ccmi_luxury_last_12_months
#
#
# next_best_share_partners
# next_best_category_moe
# next_best_category_ccmi
# next_best_category_share
# next_best_category_lifestyle
# next_best_category_that
# next_best_category_cnb
# next_best_category_lll
# next_best_category_lego
# next_best_category_als
# next_best_category_cb2
# next_best_category_anf
# next_best_category_hco
# next_best_category_shi
# next_best_category_pf
# next_best_product_lifestyle_by_brand
#
# experiences
# category_group
# store_category
# st_mall
# st_brand

