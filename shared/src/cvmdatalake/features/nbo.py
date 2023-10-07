from enum import Enum

from cvmdatalake import FeatureSpec, ColumnSpec


class NboTrainingInput(FeatureSpec):
    @classmethod
    def algorithm(cls) -> str:
        return "nbo"

    @classmethod
    def feature_type(cls) -> str:
        return "input/train"

    @classmethod
    def table_description(cls):
        return "Table contains NBO training input"

    idi_counterparty = ColumnSpec(
        data_type='string',
        description='GCR ID'
    )

    idi_proposition = ColumnSpec(
        data_type='string',
        description='Offer ID'
    )


class NboTransformInput(FeatureSpec):
    @classmethod
    def algorithm(cls) -> str:
        return "nbo"

    @classmethod
    def feature_type(cls) -> str:
        return "input/transform"

    @classmethod
    def table_description(cls):
        return "Table contains NBO transform input"

    idi_counterparty = ColumnSpec(
        data_type='string',
        description='GCR ID'
    )


class NboModel(FeatureSpec):
    @classmethod
    def algorithm(cls) -> str:
        return "nbo"

    @classmethod
    def feature_type(cls) -> str:
        return "model"

    @classmethod
    def table_description(cls):
        return "Table contains NBO transform model"


class NboOutput(FeatureSpec):
    @classmethod
    def algorithm(cls) -> str:
        return "nbo"

    @classmethod
    def feature_type(cls) -> str:
        return "output"

    @classmethod
    def table_description(cls):
        return "Table contains NBO output"

    idi_counterparty = ColumnSpec(
        data_type='string',
        description='GCR ID'
    )

    idi_proposition = ColumnSpec(
        data_type='string',
        description='Offer ID'
    )
