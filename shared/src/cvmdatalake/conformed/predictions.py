from enum import Enum

from cvmdatalake import ColumnSpec, TableSpec


class Predictions(TableSpec):

    @classmethod
    def table_description(cls):
        return "Predications from machine learning algorithms"

    idi_counterparty = ColumnSpec(
        data_type='string',
        description='Unique ID of customer (GCR ID)'
    )

    category = ColumnSpec(
        data_type='string',
        description='predications category key'
    )

    recency = ColumnSpec(
        data_type='int',
        description='Number of days since last order'
    )

    frequency = ColumnSpec(
        data_type='int',
        description='Number of  orders'
    )

    monetary = ColumnSpec(
        data_type='int',
        description='Turnover'
    )

    recency_norm = ColumnSpec(
        data_type='int',
        description='Class of recency (1-5)'
    )

    frequency_norm = ColumnSpec(
        data_type='int',
        description='Class of frequency (1-5)'
    )

    monetary_norm = ColumnSpec(
        data_type='int',
        description='Class of monetary (1-5)'
    )

    # lifecycle_stage = ColumnSpec(
    #     data_type='string',
    #     description='Lifecycle stage of customer based on RFM (New - One Time - Repeat - High Value - Lapsed - Lost - Prospect)'
    # )

    # breadth = ColumnSpec(
    #     data_type='int',
    #     description="Number of SKU's bought"
    # )

    # breadth_norm = ColumnSpec(
    #     data_type='int',
    #     description='Class of breadth (1-5)'
    # )

    next_best_offer = ColumnSpec(
        data_type='string',
        description='Predicted Next Best Offer'
    )
