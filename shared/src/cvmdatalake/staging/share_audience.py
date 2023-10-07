from enum import Enum

from cvmdatalake import TableSpec, ColumnSpec


class ShareAudience(TableSpec):

    @classmethod
    def table_description(cls):
        return "Share Audience"

    ingestion_date = ColumnSpec(
        is_partition=True,
        data_type='string',
        description='Ingestion date'
    )

    idi_audience = ColumnSpec(
        data_type='string',
        description=''
    )

    nam_audience = ColumnSpec(
        data_type='string',
        description=''
    )

    idi_counterparty = ColumnSpec(
        data_type='string',
        description=''
    )

    cod_sor_counterparty = ColumnSpec(
        data_type='string',
        description=''
    )

    idi_counterparty_gr = ColumnSpec(
        data_type='string',
        description=''
    )

    cod_sor_gr = ColumnSpec(
        data_type='string',
        description=''
    )

    dat_selection = ColumnSpec(
        data_type='date',
        description=''
    )

    type_selection = ColumnSpec(
        data_type='string',
        description=''
    )

    idi_offer = ColumnSpec(
        data_type='string',
        description=''
    )

    cod_sor_offer = ColumnSpec(
        data_type='string',
        description=''
    )

    ind_targeted = ColumnSpec(
        data_type='string',
        description=''
    )

    dat_launch_date = ColumnSpec(
        data_type='date',
        description=''
    )

    ind_coupon_clipping = ColumnSpec(
        data_type='string',
        description=''
    )

    tim_first_acceptance_ts = ColumnSpec(
        data_type='string',
        description=''
    )

    tim_last_changed_ts = ColumnSpec(
        data_type='string',
        description=''
    )

    qty_count_changed = ColumnSpec(
        data_type='string',
        description=''
    )

    ind_current_acceptance_status = ColumnSpec(
        data_type='string',
        description=''
    )

    qty_transaction_count = ColumnSpec(
        data_type='string',
        description=''
    )

    tim_first_trx_ts = ColumnSpec(
        data_type='string',
        description=''
    )

    tim_last_trx_ts = ColumnSpec(
        data_type='string',
        description=''
    )
