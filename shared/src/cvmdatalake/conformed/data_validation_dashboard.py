from enum import Enum

from cvmdatalake import TableSpec, ColumnSpec


class DataValidationDashboard(TableSpec):

    @classmethod
    def table_description(cls):
        return ""

    dataset = ColumnSpec(
        data_type='string',
        description=''
    )

    data_track_release = ColumnSpec(
        data_type='string',
        description=''
    )

    load_date = ColumnSpec(
        data_type='string',
        description=''
    )

    load_type = ColumnSpec(
        data_type='string',
        description=''
    )

    bu = ColumnSpec(
        data_type='string',
        description=''
    )

    no_of_records_as_of_load_date = ColumnSpec(
        data_type='string',
        description=''
    )

    no_of_records_as_of_current_date = ColumnSpec(
        data_type='string',
        description=''
    )

    primary_key = ColumnSpec(
        data_type='string',
        description=''
    )

    primary_key_count_landing = ColumnSpec(
        data_type='string',
        description=''
    )

    primary_key_count_staging = ColumnSpec(
        data_type='string',
        description=''
    )

    primary_key_count_conformed = ColumnSpec(
        data_type='string',
        description=''
    )

    total_count_ids_from_ttn_landing = ColumnSpec(
        data_type='string',
        description=''
    )

    total_count_ids_from_staging = ColumnSpec(
        data_type='string',
        description=''
    )

    totalhcount_ids_from_conformed = ColumnSpec(
        data_type='string',
        description=''
    )

    added_ids_to_conformed = ColumnSpec(
        data_type='string',
        description=''
    )

    total_amount_final = ColumnSpec(
        data_type='string',
        description=''
    )

    total_amount_added = ColumnSpec(
        data_type='string',
        description=''
    )

    athena_dataset_names = ColumnSpec(
        data_type='string',
        description=''
    )

    dataset_names_received_from_ttn = ColumnSpec(
        data_type='string',
        description=''
    )