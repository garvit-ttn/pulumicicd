from enum import Enum

from cvmdatalake import TableSpec, ColumnSpec


class RefSor(TableSpec):

    @classmethod
    def table_description(cls):
        return "Input REF_SOR data provided by MAF"

    cod_sor = ColumnSpec(
        data_type='string',
        description="Unique code of source, which is used as a reference in every table"
    )

    nam_sor = ColumnSpec(
        data_type='string',
        description="Name of source"
    )

    dat_created = ColumnSpec(
        data_type='string',
        description="Date of creation of this record"
    )

    dat_update = ColumnSpec(
        data_type='string',
        description="Date of last update of this record"
    )
