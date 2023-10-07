from cvmdatalake import TableSpec, ColumnSpec


class PtaSta(TableSpec):
    @classmethod
    def table_description(cls):
        return "Provides mapping from braze/external id to app_group/bu"

    ingestion_date = ColumnSpec(
        is_partition=True,
        data_type='date',
        description='Ingestion date for incremental data loading'
    )

    idi_counterparty_gr = ColumnSpec(
        data_type='string',
        description=''
    )

    cod_sor_gr = ColumnSpec(
        data_type='string',
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
