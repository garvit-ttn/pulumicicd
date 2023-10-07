from enum import Enum

from cvmdatalake import FeatureSpec, ColumnSpec


class NbomTrainingInput(FeatureSpec):
    @classmethod
    def algorithm(cls) -> str:
        return "nbom"

    @classmethod
    def feature_type(cls) -> str:
        return "input/train"

    @classmethod
    def table_description(cls):
        return "Table contains NBOM training input"


class NbomTransformInput(FeatureSpec):
    @classmethod
    def algorithm(cls) -> str:
        return "nbom"

    @classmethod
    def feature_type(cls) -> str:
        return "input/transform"

    @classmethod
    def table_description(cls):
        return "Table contains NBOM transform input"


class NbomModel(FeatureSpec):
    @classmethod
    def algorithm(cls) -> str:
        return "nbom"

    @classmethod
    def feature_type(cls) -> str:
        return "model"

    @classmethod
    def table_description(cls):
        return "Table contains NBOM transform model"


class NbomOutput(FeatureSpec):
    @classmethod
    def algorithm(cls) -> str:
        return "nbom"

    @classmethod
    def feature_type(cls) -> str:
        return "output"

    @classmethod
    def table_description(cls):
        return "Table contains NBOM output"

    idi_counterparty = ColumnSpec(
        data_type='string',
        description='GCR ID'
    )

    idi_proposition = ColumnSpec(
        data_type='string',
        description='Offer ID'
    )
