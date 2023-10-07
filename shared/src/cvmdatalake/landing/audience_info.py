from cvmdatalake import TableSpec, ColumnSpec


class AudienceInfo(TableSpec):

    @classmethod
    def table_description(cls):
        return "Table contains NBO output"

    id = ColumnSpec(
        data_type='string',
        description='Offer bank audience ID'
    )

    campaign_name = ColumnSpec(
        data_type='string',
        description=''
    )

    campaign_type = ColumnSpec(
        data_type='string',
        description=''
    )

    campaign_query = ColumnSpec(
        data_type='string',
        description=''
    )

    raw_query = ColumnSpec(
        data_type='string',
        description=''
    )

    campaign_saved = ColumnSpec(
        data_type='string',
        description=''
    )

    campaign_stored= ColumnSpec(
        data_type='string',
        description=''
    )

    draft = ColumnSpec(
        data_type='string',
        description=''
    )

    date_created = ColumnSpec(
        data_type='string',
        description=''
    )

    last_modified = ColumnSpec(
        data_type='string',
        description=''
    )

    business_unit = ColumnSpec(
        data_type='string',
        description=''
    )

    user_id_id = ColumnSpec(
        data_type='string',
        description=''
    )

    audience_count = ColumnSpec(
        data_type='string',
        description=''
    )

    uplift_clv = ColumnSpec(
        data_type='string',
        description=''
    )

    start_timestamp = ColumnSpec(
        data_type='timestamp',
        description=''
    )
