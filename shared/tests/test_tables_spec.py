from dataclasses import dataclass

import pytest

from cvmdatalake import conformed, staging, landing, features, ColumnSpec


def test_table_name():
    assert landing.Demographics.table_name() == 'demographics'
    assert conformed.CustomerProfile.table_name() == 'customer_profile'
    assert conformed.IdMapping.table_name() == 'id_mapping'


def test_table_zone():
    assert landing.Demographics.table_zone() == 'landing'
    assert staging.Demographics.table_zone() == 'staging'


def test_table_id():
    assert landing.Demographics.table_id() == 'landing.demographics'
    assert staging.Demographics.table_id() == 'staging.demographics'


def test_column_name():
    assert landing.Demographics.dat_snapshot.name == 'dat_snapshot'
    assert staging.Demographics.dat_snapshot.name == 'dat_snapshot'


def test_column_type():
    assert landing.Demographics.dat_snapshot.data_type == 'date'
    assert staging.Demographics.dat_snapshot.data_type == 'date'


def test_column_description():
    assert landing.Demographics.dat_snapshot.description == 'Snapshot date, the date on which the data was captured and exported from the System of Record (SOR) to the file'
    assert staging.Demographics.dat_snapshot.description == 'Snapshot date, the date on which the data was captured and exported from the System of Record (SOR) to the file'


def test_column_id():
    assert conformed.Demographics.dat_snapshot.column_id() == 'conformed.demographics.dat_snapshot'
    assert staging.Demographics.dat_snapshot.column_id() == 'staging.demographics.dat_snapshot'


def test_catalog_db_name():
    assert landing.Demographics.catalog_db_name('dev') == 'cvm_dev_landing_db'


def test_catalog_table_name():
    assert landing.Demographics.catalog_table_name('dev') == 'cvm_dev_landing_demographics'


def test_table_alias():
    assert landing.Demographics.alias() == 'landing_demographics'
    assert staging.Demographics.alias() == 'staging_demographics'


def test_column_alias():
    assert landing.Demographics.des_age.alias == 'landing_demographics.des_age'
    assert staging.Demographics.des_age.alias == 'staging_demographics.des_age'


def test_algorithm():
    assert features.RfmOutput.algorithm() == 'rfm'
    assert features.ClvOutput.algorithm() == 'clv'


def test_feature_type():
    assert features.RfmOutput.feature_type() == 'output'
    assert features.ClvOutput.feature_type() == 'output'


def test_feature_table_name():
    assert features.RfmOutput.table_name() == 'rfm'
    assert features.ClvOutput.table_name() == 'clv'


def test_feature_table_id():
    assert features.NboTransformInput.table_id() == 'features.nbo.input.transform'
    assert features.NboTrainingInput.table_id() == 'features.nbo.input.train'
    assert features.RfmOutput.table_id() == 'features.rfm.output'


def test_feature_catalog_db():
    assert features.NboTransformInput.catalog_db_name('test') == 'cvm_test_features_db'


def test_feature_catalog_table_name():
    assert features.NboTransformInput.catalog_table_name('test') == 'cvm_test_features_nbo_input_transform'


def test_feature_column_alias():
    assert features.NboTransformInput.idi_counterparty.alias == 'features_nbo_input_transform.idi_counterparty'
    assert features.NboTrainingInput.idi_counterparty.alias == 'features_nbo_input_train.idi_counterparty'
    assert features.NboOutput.idi_counterparty.alias == 'features_nbo_output.idi_counterparty'


def test_feature_table_alias():
    assert features.NboTransformInput.alias() == 'features_nbo_input_transform'


def test_get_column_by_name():
    assert landing.IdMapping['idi_src'].name == 'idi_src'


def test_get_column_by_name_raise_error_when_column_doesnt_exist():
    with pytest.raises(Exception, match='Cannot find field nonexisting_field in dataset landing.id_mapping'):
        assert landing.IdMapping['nonexisting_field']


def test_check_column_by_name():
    assert 'idi_src' in landing.IdMapping
    assert 'non_existing_field_name' not in landing.IdMapping


def test_column_name_can_be_used_with_spark(spark):
    df = spark.createDataFrame([
        {'idi_src': 'test'}
    ])

    from pyspark.sql.functions import col
    df = df.select(col(landing.IdMapping.idi_src.name)).collect()

    assert df[0]['idi_src'] == 'test'


def test_alias_can_be_used_with_spark(spark):
    test_df = spark.createDataFrame([
        {'idi_src': 'test'}
    ])

    from pyspark.sql.functions import col
    test_df = (
        test_df
        .alias(landing.IdMapping.alias())
        .select(col(landing.IdMapping.idi_src.alias)).collect()
    )

    assert test_df[0]['idi_src'] == 'test'


def test_alias_df_can_be_used_with_spark(spark):
    test_df = spark.createDataFrame([
        {'idi_src': 'test'}
    ])

    from pyspark.sql.functions import col
    test_df = (
        landing.IdMapping.alias_df(test_df)
        .select(col(landing.IdMapping.idi_src.alias)).collect()
    )

    assert test_df[0]['idi_src'] == 'test'


def test_alias_col_can_be_used_with_spark(spark):
    test_df = spark.createDataFrame([
        {'idi_src': 'test'}
    ])

    from pyspark.sql.functions import col
    test_df = (
        test_df
        .alias(landing.IdMapping.alias())
        .select(landing.IdMapping.idi_src.alias_col).collect()
    )

    assert test_df[0]['idi_src'] == 'test'


def test_iterate_over_columns():
    table = staging.Transactions

    for column in table.fields():
        if column.is_partition:
            print(f"Partition {column.name}-{column.data_type}")
        else:
            print(f"{column.name}-{column.data_type}")


def test_custom_attributes():
    @dataclass
    class SampleCustomAttribute:
        custom_field_1: str
        custom_field_2: int

    spec = ColumnSpec(data_type='int', custom_attributes=[
        SampleCustomAttribute(custom_field_1="a", custom_field_2=1),
        SampleCustomAttribute(custom_field_1="b", custom_field_2=2)
    ])

    custom_0 = spec.custom(SampleCustomAttribute)[0]
    assert custom_0.custom_field_1 == 'a'
    assert custom_0.custom_field_2 == 1

    custom_1 = spec.custom(SampleCustomAttribute)[1]
    assert custom_1.custom_field_1 == 'b'
    assert custom_1.custom_field_2 == 2
