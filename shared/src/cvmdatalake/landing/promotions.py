from cvmdatalake import TableSpec, ColumnSpec


class Promotions(TableSpec):

    @classmethod
    def table_description(cls):
        return "Input Activities data provided by MAF"

    bu = ColumnSpec(
        is_partition=True,
        data_type='string',
        description='Business Unit partition'
    )

    ingestion_date = ColumnSpec(
        is_partition=True,
        data_type='date',
        description='Year partition for incremental data loading'
    )

    dat_batch = ColumnSpec(
        data_type='date',
        description=''
    )

    idi_offer = ColumnSpec(
        data_type='string',
        description=''
    )

    nam_offer = ColumnSpec(
        data_type='string',
        description=''
    )

    des_offer_type = ColumnSpec(
        data_type='string',
        description=''
    )

    des_offer_value = ColumnSpec(
        data_type='string',
        description=''
    )

    des_restrictions = ColumnSpec(
        data_type='string',
        description=''
    )

    dat_period_start = ColumnSpec(
        data_type='date',
        description=''
    )

    dat_period_end = ColumnSpec(
        data_type='date',
        description=''
    )

    des_online_ofline = ColumnSpec(
        data_type='string',
        description=''
    )

    nam_sponsor = ColumnSpec(
        data_type='string',
        description=''
    )

    des_brand = ColumnSpec(
        data_type='string',
        description=''
    )

    des_industry = ColumnSpec(
        data_type='string',
        description=''
    )

    des_opco = ColumnSpec(
        data_type='string',
        description=''
    )

    cde_bu = ColumnSpec(
        data_type='string',
        description=''
    )

    des_bu = ColumnSpec(
        data_type='string',
        description=''
    )

    des_offer = ColumnSpec(
        data_type='string',
        description=''
    )

    cde_offer_type = ColumnSpec(
        data_type='string',
        description=''
    )

    cde_country_key = ColumnSpec(
        data_type='string',
        description=''
    )

    cde_sponsor_key = ColumnSpec(
        data_type='string',
        description=''
    )

    des_url_logo = ColumnSpec(
        data_type='string',
        description=''
    )

    ind_member_visible = ColumnSpec(
        data_type='string',
        description=''
    )

    ind_offer_acceptance_required = ColumnSpec(
        data_type='string',
        description=''
    )

    des_url_image = ColumnSpec(
        data_type='string',
        description=''
    )

    des_offer_section = ColumnSpec(
        data_type='string',
        description=''
    )

    des_terms = ColumnSpec(
        data_type='string',
        description=''
    )

    des_status = ColumnSpec(
        data_type='string',
        description=''
    )

    ind_is_template_based = ColumnSpec(
        data_type='string',
        description=''
    )

    des_url_mobile_image = ColumnSpec(
        data_type='string',
        description=''
    )

    des_url_landing_page = ColumnSpec(
        data_type='string',
        description=''
    )

    des_company = ColumnSpec(
        data_type='string',
        description=''
    )

    idi_promotion_code = ColumnSpec(
        data_type='string',
        description=''
    )

    des_subtitle = ColumnSpec(
        data_type='string',
        description=''
    )

    qty_min = ColumnSpec(
        data_type='string',
        description=''
    )

    qty_max = ColumnSpec(
        data_type='string',
        description=''
    )

    ind_can_have_other_discount = ColumnSpec(
        data_type='string',
        description=''
    )

    ind_apply_after_tax = ColumnSpec(
        data_type='string',
        description=''
    )

    ind_can_apply_to_tab = ColumnSpec(
        data_type='string',
        description=''
    )

    ind_applies_when_purchasing_products = ColumnSpec(
        data_type='string',
        description=''
    )

    ind_applies_to_products = ColumnSpec(
        data_type='string',
        description=''
    )

