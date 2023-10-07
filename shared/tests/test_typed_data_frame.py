import pytest

from cvmdatalake import TableSpec, ColumnSpec
from cvmdatalake.spark_extensions import TypedDataFrame


class SampleSchema(TableSpec):
    idi_src = ColumnSpec(
        data_type='string',
        description='test field'
    )


@pytest.fixture(scope="session")
def sample_typed_df(spark) -> TypedDataFrame[SampleSchema]:
    df = spark.createDataFrame([{'idi_src': 'test'}])
    return TypedDataFrame[SampleSchema](df)


def test_typed_df_can_be_used_as_an_ordinary_df(sample_typed_df):
    sample_typed_df.show()


def test_typed_df_can_access_column_name(sample_typed_df):
    assert sample_typed_df.spec.idi_src.name == 'idi_src'


def test_typed_df_can_access_alias(sample_typed_df):
    assert sample_typed_df.spec.idi_src.alias == 'test_typed_data_frame_sample_schema.idi_src'
