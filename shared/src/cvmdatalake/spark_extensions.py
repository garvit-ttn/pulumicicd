from typing import Type, TypeVar, Generic, get_args

from pyspark.sql import DataFrame

from cvmdatalake import TableSpec, creat_delta_table_if_not_exists, get_s3_path


# noinspection PyProtectedMember
def get_show_str(df: DataFrame, n=20, truncate=True, vertical=False):
    return (
        df._jdf.showString(n, 20, vertical)
        if isinstance(truncate, bool) and truncate
        else df._jdf.showString(n, int(truncate), vertical)
    )


def write_delta(
        df: DataFrame, table: Type[TableSpec],
        spark, lake_descriptor: str,
        save_mode: str | None, overwrite_schema=True
) -> None:
    creat_delta_table_if_not_exists(spark, table, lake_descriptor)
    delta_path = get_s3_path(table, lake_descriptor)

    return (
        df.write.format('delta')
        .option("overwriteSchema", str(overwrite_schema).lower())
        .mode(save_mode)
        .save(delta_path)
    )


TTableSpec = TypeVar("TTableSpec", bound=TableSpec)


class TypedDataFrame(Generic[TTableSpec], DataFrame):
    def __init__(self, df: DataFrame):
        super().__init__(df._jdf, df.sql_ctx)
        # TODO: verify if spec matches df schema.
        # even though it can be only evaluated in runtime it's still worth to log the differences if any

    @property
    def spec(self) -> Type[TTableSpec]:
        return get_args(self.__orig_class__)[0]
