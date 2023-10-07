from enum import Enum

from cvmdatalake import TableSpec, ColumnSpec


class ProfileMeasurements(TableSpec):

    @classmethod
    def table_description(cls):
        return "Table for Measurements split for gcr"

    idi_counterparty = ColumnSpec(
        data_type='string',
        description='The grc_id of the counterparty'
    )

    idi_measurement = ColumnSpec(
        data_type='string',
        description='result',
    )

    task = ColumnSpec(
        data_type='string',
        description='For tracking'
    )

    run_id = ColumnSpec(
        data_type='string',
        description='id'
    )

    run_date = ColumnSpec(
        data_type='date',
        description=''
    )
