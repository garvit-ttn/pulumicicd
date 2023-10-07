from enum import Enum

from cvmdatalake import TableSpec, ColumnSpec


class Offers(TableSpec):

    @classmethod
    def table_description(cls):
        return "Table contains offers data from offers bank"

    offer_id = ColumnSpec(
        data_type='string',
        description=''
    )

    cta_link = ColumnSpec(
        data_type='string',
        description=''
    )

    brand = ColumnSpec(
        data_type='string',
        description=''
    )

    directory_name = ColumnSpec(
        data_type='string',
        description=''
    )

    offer_title = ColumnSpec(
        data_type='string',
        description=''
    )

    offer_subtitle = ColumnSpec(
        data_type='string',
        description=''
    )

    offer_description = ColumnSpec(
        data_type='string',
        description=''
    )

    tcs = ColumnSpec(
        data_type='string',
        description=''
    )

    target_audience = ColumnSpec(
        data_type='string',
        description=''
    )

    offer_start_date = ColumnSpec(
        data_type='string',
        description=''
    )

    offer_expiry_date = ColumnSpec(
        data_type='string',
        description=''
    )

    limit_per_member = ColumnSpec(
        data_type='string',
        description=''
    )

    locations = ColumnSpec(
        data_type='string',
        description=''
    )

    image_file_name = ColumnSpec(
        data_type='string',
        description=''
    )

    home_large_image_thumbnail = ColumnSpec(
        data_type='string',
        description=''
    )

    offer_image = ColumnSpec(
        data_type='string',
        description=''
    )

    header_image = ColumnSpec(
        data_type='string',
        description=''
    )

    main_image = ColumnSpec(
        data_type='string',
        description=''
    )
