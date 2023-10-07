from cvmdatalake import TableSpec, ColumnSpec


class LoyaltyBusinessUnit(TableSpec):
    @classmethod
    def table_description(cls):
        return "Provides mapping from braze/external id to app_group/bu"

    ingestion_date = ColumnSpec(
        is_partition=True,
        data_type='date',
        description='Ingestion date for incremental data loading'
    )

    bu_key = ColumnSpec(
        data_type='bigint',
        description=''
    )

    code = ColumnSpec(
        data_type='string',
        description=''
    )

    bu_name = ColumnSpec(
        data_type='string',
        description=''
    )

    opco_key = ColumnSpec(
        data_type='bigint',
        description=''
    )

    active_flag = ColumnSpec(
        data_type='boolean',
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
