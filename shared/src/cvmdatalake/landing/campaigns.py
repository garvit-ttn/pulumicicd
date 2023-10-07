from cvmdatalake import TableSpec, ColumnSpec


class Campaign(TableSpec):

    @classmethod
    def table_description(cls):
        return "Table contains campaign details from Offer's Bank backend"

    audience_id = ColumnSpec(
        data_type='string'
    )

    audiences_concat = ColumnSpec(
        data_type='string'
    )

    offer_concat = ColumnSpec(
        data_type='string'
    )

    name = ColumnSpec(
        data_type='string'
    )

    active = ColumnSpec(
        data_type='boolean'
    )

    pending = ColumnSpec(
        data_type='boolean'
    )

    draft = ColumnSpec(
        data_type='boolean'
    )

    processed = ColumnSpec(
        data_type='boolean'
    )

    start_date = ColumnSpec(
        data_type='string'
    )

    end_date = ColumnSpec(
        data_type='string'
    )

    touchpoints = ColumnSpec(
        data_type='string'
    )

    created_at = ColumnSpec(
        data_type='timestamp'
    )

    status = ColumnSpec(
        data_type='string'
    )

