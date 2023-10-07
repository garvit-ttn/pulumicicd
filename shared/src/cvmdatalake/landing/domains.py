from enum import Enum

from cvmdatalake import TableSpec, ColumnSpec


class Domains(TableSpec):

    @classmethod
    def table_description(cls):
        return "Input Domains data provided by MAF"

    dat_snapshot = ColumnSpec(
        data_type='string',
        description="Snapshot date, the date on which the data was captured and exported from the System of Record (SOR) to the file. The Snapshot date is the same for every record in the File"
    )

    cdi_domain_attribute = ColumnSpec(
        data_type='string',
        description="The attribute name delivered in DTI Protocol where code / value pair domain is used"
    )

    cdi_domain_value = ColumnSpec(
        data_type='string',
        description="The code of the domain (M = Male, F = Female or 1 = Active, 2 is Not Active, etc…)"
    )

    cdi_domain_value_description = ColumnSpec(
        data_type='string',
        description="The description of the Code"
    )
