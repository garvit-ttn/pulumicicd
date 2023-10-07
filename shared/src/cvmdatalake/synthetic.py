import random
from dataclasses import dataclass
from typing import Type, Any, Callable

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import to_date
from pyspark.sql.types import *

from cvmdatalake import TableSpec


@dataclass
class FakerMapping:
    method: str
    args: dict[str, Any] | None = None


_type_to_fake_map = {
    'string': 'pystr',
    'date': 'date',
    'int': 'pyint',
    'bigint': 'pyint',
    'float': 'pyfloat',
    'double': 'pyfloat'
}

_type_to_pyspark_map = {
    'string': StringType(),
    'date': StringType(),  # this is intentional. we will cast to DateType() later
    'int': IntegerType(),
    'bigint': LongType(),
    'float': FloatType(),
    'double': DoubleType()
}


def fake_df(
        spark: SparkSession, spec: Type[TableSpec], n=100,
        n_fake_string_columns=0, n_fake_float_columns=0
) -> DataFrame:
    def fake_row(partition_rows):
        from faker import Faker
        f = Faker()

        sample_bus = ['test_bu_01', 'test_bu_02', 'test_bu_03']
        single_row_generators: list[Callable] = []

        for column in spec:
            # We want to limit the cardinality of partitioning columns
            # TODO: extend for other types of partitioning columns based on types
            if column.is_partition and column.name == 'bu':
                single_row_generators.append(lambda: random.choice(sample_bus))
                continue

            # retrieve custom FakeMapping attribute describing fake method to be used for each column
            mapping = column.custom(FakerMapping)
            if len(mapping) > 0:
                faker_method_name = mapping[0].method
            else:
                faker_method_name = _type_to_fake_map[column.data_type]

            # append fake method to generators
            faker_method = getattr(f, faker_method_name)
            single_row_generators.append(faker_method)

        # prepare generators for default fake columns
        for _ in range(0, n_fake_string_columns):
            single_row_generators.append(f.pystr)

        for _ in range(0, n_fake_float_columns):
            single_row_generators.append(f.pyfloat)

        # generate rows
        for _ in partition_rows:
            yield [gen() for gen in single_row_generators]

    # prepare final schema with default fake columns
    fake_schema = spark_schema(spec)
    for i in range(0, n_fake_string_columns):
        fake_schema.add(field=f"fake_col_{i + 1}", data_type=StringType(), nullable=True)

    for i in range(n_fake_string_columns, n_fake_string_columns + n_fake_float_columns):
        fake_schema.add(field=f"fake_col_{i + 1}", data_type=StringType(), nullable=True)

    # generate fake sample
    sample_df = spark.range(0, n).rdd.mapPartitions(fake_row).toDF(fake_schema)

    # cast all date columns
    for c in spec:
        if c.data_type == 'date':
            sample_df = sample_df.withColumn(c.name, to_date(c.name))

    return sample_df


def spark_schema(spec: Type[TableSpec]) -> StructType:
    return StructType([
        StructField(
            name=column.name,
            dataType=_type_to_pyspark_map[column.data_type],
            nullable=True
        ) for column in spec
    ])
