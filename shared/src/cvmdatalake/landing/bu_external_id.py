from cvmdatalake import TableSpec, ColumnSpec


class BuExternalId(TableSpec):
    @classmethod
    def table_description(cls):
        return "Provides mapping from braze/external id to app_group/bu"

    ingestion_date = ColumnSpec(
        is_partition=True,
        data_type='date',
        description='Ingestion date for incremental data loading'
    )

    gcr_id = ColumnSpec(
        data_type='string',
        description='GCR id'
    )

    external_id = ColumnSpec(
        data_type='string',
        description='Braze id'
    )

    org_code = ColumnSpec(
        data_type='string',
        description='app group'
    )

    brands = ColumnSpec(
        data_type='string',
        description='brand'
    )
