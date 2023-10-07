from cvmdatalake import TableSpec, ColumnSpec


class BuExternalId(TableSpec):
    @classmethod
    def table_description(cls):
        return "Provides mapping from braze/external id to app_group/bu"

    gcr_id = ColumnSpec(
        data_type='string',
        description='GCR Id'
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
