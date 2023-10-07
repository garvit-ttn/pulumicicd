from enum import Enum

from cvmdatalake import ColumnSpec, FeatureSpec


class MbaTrainingInput(FeatureSpec):
    @classmethod
    def algorithm(cls) -> str:
        return "mba"

    @classmethod
    def feature_type(cls) -> str:
        return "input/train"

    @classmethod
    def table_description(cls):
        return "Table contains MBA training  input"


class MbaTransformInput(FeatureSpec):
    @classmethod
    def algorithm(cls) -> str:
        return "mba"

    @classmethod
    def feature_type(cls) -> str:
        return "input/transform"

    @classmethod
    def table_description(cls):
        return "Table contains MBA training input"


class MbaModel(FeatureSpec):
    @classmethod
    def algorithm(cls) -> str:
        return "mba"

    @classmethod
    def feature_type(cls) -> str:
        return "model"

    @classmethod
    def table_description(cls):
        return "Table contains MBA training model"


class MbaOutput(FeatureSpec):
    @classmethod
    def algorithm(cls) -> str:
        return "mba"

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

    min_date = ColumnSpec(
        data_type='date',
        description=''
    )

    max_date = ColumnSpec(
        data_type='date',
        description=''
    )

    frequency = ColumnSpec(
        data_type='float',
        description=''
    )

    monetary = ColumnSpec(
        data_type='float',
        description=''
    )

    age = ColumnSpec(
        data_type='bigint',
        description=''
    )

    lor = ColumnSpec(
        data_type='string',
        description=''
    )

    p_alive = ColumnSpec(
        data_type='float',
        description=''
    )
