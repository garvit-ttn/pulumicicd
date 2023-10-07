from enum import Enum

from cvmdatalake import TableSpec, ColumnSpec


class Location(TableSpec):

    @classmethod
    def table_description(cls):
        return "Input Location data provided by MAF"

    bu = ColumnSpec(
        is_partition=True,
        data_type='string',
        description='Business Unit'
    )

    adr_postal_address_string = ColumnSpec(
        is_partition=False,
        data_type='string',
        description=''
    )

    des_country_name = ColumnSpec(
        is_partition=False,
        data_type='string',
        description=''
    )

    cde_base_bu_key = ColumnSpec(
        is_partition=False,
        data_type='string',
        description=''
    )

    cde_base_opco_key = ColumnSpec(
        is_partition=False,
        data_type='string',
        description=''
    )

    cde_base_sponsor_key = ColumnSpec(
        is_partition=False,
        data_type='string',
        description=''
    )

    cde_country_key = ColumnSpec(
        is_partition=False,
        data_type='string',
        description=''
    )

    cde_layered_sponsor_key = ColumnSpec(
        is_partition=False,
        data_type='string',
        description=''
    )

    cde_store_code = ColumnSpec(
        is_partition=False,
        data_type='string',
        description=''
    )

    cde_store_key = ColumnSpec(
        is_partition=False,
        data_type='string',
        description=''
    )

    des_cityname = ColumnSpec(
        is_partition=False,
        data_type='string',
        description=''
    )

    des_location = ColumnSpec(
        is_partition=False,
        data_type='string',
        description=''
    )

    des_maf_brand_name = ColumnSpec(
        is_partition=False,
        data_type='string',
        description=''
    )

    des_mall_name = ColumnSpec(
        is_partition=False,
        data_type='string',
        description=''
    )

    des_ofline_online = ColumnSpec(
        is_partition=False,
        data_type='string',
        description=''
    )

    des_opco = ColumnSpec(
        is_partition=False,
        data_type='string',
        description=''
    )

    des_store_name = ColumnSpec(
        is_partition=False,
        data_type='string',
        description=''
    )

    idi_ecp_tenant_store_id = ColumnSpec(
        is_partition=False,
        data_type='string',
        description=''
    )

    idi_store_hash_id = ColumnSpec(
        is_partition=False,
        data_type='string',
        description=''
    )

    des_src = ColumnSpec(
        is_partition=False,
        data_type='string',
        description=''
    )

    idi_ecp_store_id = ColumnSpec(
        is_partition=False,
        data_type='string',
        description=''
    )

    idi_mall_id = ColumnSpec(
        is_partition=False,
        data_type='string',
        description=''
    )

    ind_active_flag = ColumnSpec(
        is_partition=False,
        data_type='string',
        description=''
    )

    idi_ecp_mall_id = ColumnSpec(
        is_partition=False,
        data_type='string',
        description=''
    )

    idi_ecp_mall_hash_id = ColumnSpec(
        is_partition=False,
        data_type='string',
        description=''
    )

    idi_city_id = ColumnSpec(
        is_partition=False,
        data_type='string',
        description=''
    )

    des_location_display_2 = ColumnSpec(
        is_partition=False,
        data_type='string',
        description=''
    )

    ind_is_maf_mall = ColumnSpec(
        is_partition=False,
        data_type='string',
        description=''
    )

    des_store_category = ColumnSpec(
        is_partition=False,
        data_type='string',
        description=''
    )

    des_category_group = ColumnSpec(
        is_partition=False,
        data_type='string',
        description=''
    )

    des_store_experience = ColumnSpec(
        is_partition=False,
        data_type='string',
        description=''
    )

    des_segment_type = ColumnSpec(
        is_partition=False,
        data_type='string',
        description=''
    )

    idi_city_key = ColumnSpec(
        is_partition=False,
        data_type='string',
        description=''
    )

    idi_area_key = ColumnSpec(
        is_partition=False,
        data_type='string',
        description=''
    )

    idi_state_key = ColumnSpec(
        is_partition=False,
        data_type='string',
        description=''
    )

    cde_latitude = ColumnSpec(
        is_partition=False,
        data_type='string',
        description=''
    )

    cde_longitude = ColumnSpec(
        is_partition=False,
        data_type='string',
        description=''
    )

    idi_region_key = ColumnSpec(
        is_partition=False,
        data_type='string',
        description=''
    )

    des_region_name = ColumnSpec(
        is_partition=False,
        data_type='string',
        description=''
    )

    des_sponsor_name = ColumnSpec(
        is_partition=False,
        data_type='string',
        description=''
    )

    des_bu_name = ColumnSpec(
        is_partition=False,
        data_type='string',
        description=''
    )

    des_stype_name = ColumnSpec(
        is_partition=False,
        data_type='string',
        description=''
    )

    dat_open_date = ColumnSpec(
        is_partition=False,
        data_type='string',
        description=''
    )

    dat_close_date = ColumnSpec(
        is_partition=False,
        data_type='string',
        description=''
    )

    des_district_name = ColumnSpec(
        is_partition=False,
        data_type='string',
        description=''
    )

    des_store_desc = ColumnSpec(
        is_partition=False,
        data_type='string',
        description=''
    )

    des_company = ColumnSpec(
        is_partition=False,
        data_type='string',
        description=''
    )

    dat_working_hours = ColumnSpec(
        is_partition=False,
        data_type='string',
        description=''
    )

    idi_brand_code = ColumnSpec(
        is_partition=False,
        data_type='string',
        description=''
    )

    dat_opening_hours = ColumnSpec(
        is_partition=False,
        data_type='string',
        description=''
    )

    dat_closing_hours = ColumnSpec(
        is_partition=False,
        data_type='string',
        description=''
    )