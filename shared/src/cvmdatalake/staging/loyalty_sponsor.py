from cvmdatalake import TableSpec, ColumnSpec


class LoyaltySponsor(TableSpec):
    @classmethod
    def table_description(cls):
        return "Provides mapping from braze/external id to app_group/bu"

    sponsor_key = ColumnSpec(
        data_type='int',
        description=''
    )

    sponsor_name = ColumnSpec(
        data_type='string',
        description=''
    )

    short_name = ColumnSpec(
        data_type='string',
        description=''
    )

    industry_key = ColumnSpec(
        data_type='int',
        description=''
    )

    opco_key = ColumnSpec(
        data_type='int',
        description=''
    )

    bu_key = ColumnSpec(
        data_type='int',
        description=''
    )

    sponsor_stage = ColumnSpec(
        data_type='string',
        description=''
    )

    payment_status = ColumnSpec(
        data_type='string',
        description=''
    )

    description = ColumnSpec(
        data_type='string',
        description=''
    )

    country_key = ColumnSpec(
        data_type='int',
        description=''
    )

    city_key = ColumnSpec(
        data_type='int',
        description=''
    )

    address = ColumnSpec(
        data_type='string',
        description=''
    )

    phone_number = ColumnSpec(
        data_type='string',
        description=''
    )

    contact_person_name = ColumnSpec(
        data_type='string',
        description=''
    )

    email = ColumnSpec(
        data_type='string',
        description=''
    )

    url = ColumnSpec(
        data_type='string',
        description=''
    )

    acquisition_url = ColumnSpec(
        data_type='string',
        description=''
    )

    start_date = ColumnSpec(
        data_type='timestamp',
        description=''
    )

    end_date = ColumnSpec(
        data_type='timestamp',
        description=''
    )

    created_ts = ColumnSpec(
        data_type='timestamp',
        description=''
    )

    updated_ts = ColumnSpec(
        data_type='timestamp',
        description=''
    )

    ods_processing_ts = ColumnSpec(
        data_type='timestamp',
        description=''
    )

    grid_ins_ts = ColumnSpec(
        data_type='timestamp',
        description=''
    )

    grid_upd_ts = ColumnSpec(
        data_type='timestamp',
        description=''
    )

    active_status = ColumnSpec(
        data_type='boolean',
        description=''
    )

    maf_brand = ColumnSpec(
        data_type='string',
        description=''
    )

    country_name = ColumnSpec(
        data_type='string',
        description=''
    )
