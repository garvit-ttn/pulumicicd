from enum import Enum

from cvmdatalake import TableSpec, ColumnSpec


class IdMapping(TableSpec):

    @classmethod
    def table_description(cls):
        return "IDs mapping from business unit specific IDs to GCR"

    cod_sor_idi_gcr = ColumnSpec(
        data_type="string",
        description='GCR ID'
    )

    idi_gcr = ColumnSpec(
        data_type='string',
        description='GCR ID'
    )

    cod_sor_idi_src = ColumnSpec(
        data_type='string',
        description='ID within source'
    )

    idi_src = ColumnSpec(
        data_type='string',
        description='ID within source'
    )

    dat_created = ColumnSpec(
        data_type='date',
        description='Date of creation of this record'
    )

    dat_update = ColumnSpec(
        data_type='date',
        description='Date of last update of this record'
    )
