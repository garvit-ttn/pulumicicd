from enum import Enum

from cvmdatalake import ColumnSpec, FeatureSpec


class RfmTrainingInput(FeatureSpec):
    @classmethod
    def algorithm(cls) -> str:
        return "rfm"

    @classmethod
    def feature_type(cls) -> str:
        return "input/train"

    @classmethod
    def table_description(cls):
        return "Table contains RFM training input"


class RfmTransformInput(FeatureSpec):
    @classmethod
    def algorithm(cls) -> str:
        return "rfm"

    @classmethod
    def feature_type(cls) -> str:
        return "input/transform"

    @classmethod
    def table_description(cls):
        return "Table contains RFM transform input"


class RfmModel(FeatureSpec):
    @classmethod
    def algorithm(cls) -> str:
        return "rfm"

    @classmethod
    def feature_type(cls) -> str:
        return "model"

    @classmethod
    def table_description(cls):
        return "Table contains RFM transform model"


class RfmOutput(FeatureSpec):
    @classmethod
    def algorithm(cls) -> str:
        return "rfm"

    @classmethod
    def feature_type(cls) -> str:
        return "output"

    @classmethod
    def table_description(cls):
        return "Table contains RFM output"

    idi_counterparty = ColumnSpec(
        data_type='string',
        description='GCR ID'
    )

    idi_proposition_level03 = ColumnSpec(
        data_type='string',
        description='Offer ID'
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

    r_rank_norm = ColumnSpec(
        data_type='int',
        description='Class of recency (1-5)'
    )

    f_rank_norm = ColumnSpec(
        data_type='int',
        description='Class of frequency (1-5)'
    )

    m_rank_norm = ColumnSpec(
        data_type='int',
        description='Class of monetary (1-5)'
    )

    customer_segment = ColumnSpec(
        data_type='string',
        description='Class of monetary (1-5)'
    )
