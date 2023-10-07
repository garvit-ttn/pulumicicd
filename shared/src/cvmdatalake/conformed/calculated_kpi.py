from enum import Enum

from cvmdatalake import ColumnSpec, TableSpec


class ProfileCalculatedKpi(TableSpec):

    @classmethod
    def table_description(cls):
        return "Calculated KPIs des"

    idi_counterparty_gr = ColumnSpec(
        data_type='string',
        description=''
    )

    sow = ColumnSpec(
        data_type='int',
        description=' "MONETARY_3M" / "MONETARY_3M_NI" * 100 - calculated in job'
    )

    sow_moe = ColumnSpec(
        data_type='int',
        description='MOE -> "MONETARY_3M" / "MONETARY_3M_NI" * 100'
    )

    sow_cc_d = ColumnSpec(
        data_type='int',
        description='CC_D -> "MONETARY_3M" / "MONETARY_3M_NI" * 100'
    )

    pta = ColumnSpec(
        data_type='string',
        description='We take this parameter from vertica upload folder'
    )

    sta = ColumnSpec(
        data_type='string',
        description='We take this parameter from vertica upload folder'
    )

    turnover_3m = ColumnSpec(
        data_type='int',
        description='Calculated Monthly Turnover for last 3 months - calculated in job'
    )

    turnover_6m = ColumnSpec(
        data_type='int',
        description='Calculated Monthly Turnover for last 6 months - calculated in job'
    )

    turnover_12m = ColumnSpec(
        data_type='int',
        description='Calculated Monthly Turnover for last 12 months - calculated in job'
    )

    turnover_3m_moe = ColumnSpec(
        data_type='int',
        description='Calculated Monthly Turnover for last 3 months for NI MOE - calculated in job'
    )

    turnover_3m_ni = ColumnSpec(
        data_type='int',
        description='Calculated Monthly Turnover for last 3 months for NI - calculated in job'
    )

    engagement_status = ColumnSpec(
        data_type='string',
        description='Or different name CHURN_STATUS. States: WARNING, AT RISK, OK - calculated in job'
    )

    bu_turnover = ColumnSpec(
        data_type='string',
        description='Contact cde_base_bu_key from  transactions - calculated in job'
    )

    ni_locations = ColumnSpec(
        data_type='string',
        description='Contact des_location from transactions for NI - calculated in job'
    )

    number_of_visits_12m = ColumnSpec(
        data_type='int',
        description='NUMBER_OF_VISITS for 12M - calculated in job'
    )
    total_spend_als_last_3_months = ColumnSpec(
        data_type='double',
        description=''
    )

    total_spend_als_last_6_months = ColumnSpec(
        data_type='double',
        description=''
    )

    total_spend_carrefour_last_3_months = ColumnSpec(
        data_type='double',
        description=''
    )

    total_spend_carrefour_last_6_months = ColumnSpec(
        data_type='double',
        description=''
    )

    total_spend_ccd_last_3_months = ColumnSpec(
        data_type='double',
        description=''
    )

    total_spend_ccd_last_6_months = ColumnSpec(
        data_type='double',
        description=''
    )

    total_spend_ccmi_last_3_months = ColumnSpec(
        data_type='double',
        description=''
    )

    total_spend_ccmi_last_6_months = ColumnSpec(
        data_type='double',
        description=''
    )

    total_spend_cnb_last_3_months = ColumnSpec(
        data_type='double',
        description=''
    )

    total_spend_cnb_last_6_months = ColumnSpec(
        data_type='double',
        description=''
    )

    total_spend_crf_last_3_months = ColumnSpec(
        data_type='double',
        description=''
    )

    total_spend_crf_last_6_months = ColumnSpec(
        data_type='double',
        description=''
    )

    total_spend_lec_last_3_months = ColumnSpec(
        data_type='double',
        description=''
    )

    total_spend_lec_last_6_months = ColumnSpec(
        data_type='double',
        description=''
    )

    total_spend_lifestyle_last_3_months = ColumnSpec(
        data_type='double',
        description=''
    )

    total_spend_lifestyle_last_6_months = ColumnSpec(
        data_type='double',
        description=''
    )

    total_spend_lll_last_3_months = ColumnSpec(
        data_type='double',
        description=''
    )

    total_spend_lll_last_6_months = ColumnSpec(
        data_type='double',
        description=''
    )

    total_spend_magicp_last_3_months = ColumnSpec(
        data_type='double',
        description=''
    )

    total_spend_moe_last_12_months = ColumnSpec(
        data_type='double',
        description=''
    )

    total_spend_moe_last_1_day = ColumnSpec(
        data_type='double',
        description=''
    )

    total_spend_moe_last_1_month = ColumnSpec(
        data_type='double',
        description=''
    )

    total_spend_moe_last_3_months = ColumnSpec(
        data_type='double',
        description=''
    )

    total_spend_moe_last_6_months = ColumnSpec(
        data_type='double',
        description=''
    )

    total_spend_moe_last_7_days = ColumnSpec(
        data_type='double',
        description=''
    )

    total_spend_share_last_3_months = ColumnSpec(
        data_type='double',
        description=''
    )

    total_spend_share_last_6_months = ColumnSpec(
        data_type='double',
        description=''
    )

    total_spend_share_last_7_days = ColumnSpec(
        data_type='double',
        description=''
    )

    total_spend_skidub_last_3_months = ColumnSpec(
        data_type='double',
        description=''
    )

    total_spend_skidub_last_6_months = ColumnSpec(
        data_type='double',
        description=''
    )

    total_spend_smbu_last_3_months = ColumnSpec(
        data_type='double',
        description=''
    )

    total_spend_smbu_last_6_months = ColumnSpec(
        data_type='double',
        description=''
    )

    total_spend_vox_last_3_months = ColumnSpec(
        data_type='double',
        description=''
    )

    total_spend_vox_last_6_months = ColumnSpec(
        data_type='double',
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
        data_type='double',
        description=''
    )

    total_spend_share_partner_last_7_days = ColumnSpec(
        data_type='double',
        description=''
    )

    total_spend_share_partner_last_1_month = ColumnSpec(
        data_type='double',
        description=''
    )

    total_spend_share_partner_last_3_months = ColumnSpec(
        data_type='double',
        description=''
    )

    total_spend_share_partner_last_6_months = ColumnSpec(
        data_type='double',
        description=''
    )

    total_spend_share_partner_last_12_months = ColumnSpec(
        data_type='double',
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

    total_spend_moe_luxury_last_7_days = ColumnSpec(
        data_type='double',
        description=''
    )

    total_spend_moe_luxury_last_1_month = ColumnSpec(
        data_type='double',
        description=''
    )

    total_spend_moe_luxury_last_3_months = ColumnSpec(
        data_type='double',
        description=''
    )

    total_spend_moe_luxury_last_6_months = ColumnSpec(
        data_type='double',
        description=''
    )

    total_spend_moe_luxury_last_12_months = ColumnSpec(
        data_type='double',
        description=''
    )

    total_spend_ccmi_luxury_last_7_days = ColumnSpec(
        data_type='double',
        description=''
    )

    total_spend_ccmi_luxury_last_1_month = ColumnSpec(
        data_type='double',
        description=''
    )

    total_spend_ccmi_luxury_last_3_months = ColumnSpec(
        data_type='double',
        description=''
    )

    total_spend_ccmi_luxury_last_6_months = ColumnSpec(
        data_type='double',
        description=''
    )

    total_spend_ccmi_luxury_last_12_months = ColumnSpec(
        data_type='double',
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
