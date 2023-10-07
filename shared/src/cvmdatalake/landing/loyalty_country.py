from cvmdatalake import TableSpec, ColumnSpec


class LoyaltyCountry(TableSpec):
    @classmethod
    def table_description(cls):
        return "Provides mapping from braze/external id to app_group/bu"

    ingestion_date = ColumnSpec(
        is_partition=True,
        data_type='date',
        description='Ingestion date for incremental data loading'
    )

    name = ColumnSpec(
        data_type='string',
        description=''
    )

    alpha_2 = ColumnSpec(
        data_type='string',
        description=''
    )

    alpha_3 = ColumnSpec(
        data_type='string',
        description=''
    )

    country_code = ColumnSpec(
        data_type='bigint',
        description=''
    )

    iso_3166_2 = ColumnSpec(
        data_type='string',
        description=''
    )

    region = ColumnSpec(
        data_type='string',
        description=''
    )

    sub_region = ColumnSpec(
        data_type='string',
        description=''
    )

    intermediate_region = ColumnSpec(
        data_type='string',
        description=''
    )

    region_code = ColumnSpec(
        data_type='bigint',
        description=''
    )

    sub_region_code = ColumnSpec(
        data_type='bigint',
        description=''
    )

    intermediate_region_code = ColumnSpec(
        data_type='bigint',
        description=''
    )