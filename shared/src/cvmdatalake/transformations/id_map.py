from enum import Enum

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from cvmdatalake.conformed import IdMapping


# TODO: add a lot of unit tests for these functions. These are probably the most critical for all transformations

def enrich_with_customer_gcr_id(
        input_df: DataFrame, id_map: DataFrame,
        input_idi_src_col=IdMapping.idi_src.name,
        output_gcr_col=IdMapping.idi_gcr.name
) -> DataFrame:
    id_map = id_map.select(
        IdMapping.idi_gcr.name,
        IdMapping.idi_src.name
    )

    if not input_idi_src_col.startswith('input.'):
        input_idi_src_col = f"input.{input_idi_src_col}"

    output_df = (
        input_df.alias('input')
        .join(
            how='inner', other=IdMapping.alias_df(id_map),
            on=col(input_idi_src_col) == col(IdMapping.idi_src.alias)
        )
        .drop(col(IdMapping.idi_src.alias))
    )

    if output_gcr_col != IdMapping.idi_gcr.name:
        output_df = output_df.withColumn(output_gcr_col, col(IdMapping.idi_gcr.alias))
        output_df = output_df.drop(col(IdMapping.idi_gcr.alias))

    return output_df


class SourceSystem(Enum):
    Braze = "971013"
    Share = "971005"
    Gcr = "971002"


def enrich_with_customer_src_id(
        input_df: DataFrame, id_map: DataFrame, source: SourceSystem,
        input_idi_gcr_col=IdMapping.idi_gcr.name,
        output_src_col=IdMapping.idi_src.name
) -> DataFrame:
    src_id_map = id_map.filter(col(IdMapping.cod_sor_idi_src.name) == source.value).select([
        IdMapping.idi_gcr.name,
        IdMapping.idi_src.name
    ])

    if not input_idi_gcr_col.startswith('input.'):
        input_idi_gcr_col = f"input.{input_idi_gcr_col}"

    output_df = (
        input_df.alias('input')
        .join(
            how='inner', other=IdMapping.alias_df(src_id_map),
            on=col(input_idi_gcr_col) == col(IdMapping.idi_gcr.alias)
        )
        .drop(col(IdMapping.idi_gcr.alias))
    )

    if output_src_col != IdMapping.idi_src.name:
        output_df = output_df.withColumn(output_src_col, col(IdMapping.idi_src.alias))
        output_df = output_df.drop(col(IdMapping.idi_src.alias))

    return output_df
