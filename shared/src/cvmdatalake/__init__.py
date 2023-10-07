import json
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Type, Optional, Sequence, cast, TypeVar, Any

from awsglue import DynamicFrame
from pyspark.sql import functions as f, SparkSession, DataFrame, Column
from pyspark.sql.functions import col

from cvmdatalake.data_filters import DataFilterContext


class Owner(Enum):
    Exsell = 'Exsell'


class Stage(Enum):
    Ingestion = 'ingestion'
    DataQuality = 'dq'
    FeaturePrep = 'features'
    Enrichment = 'enrich_c360'
    ApiExtract = 'api_extract'
    ControlTowerExtract = 'ct_extract'
    OffersLoad = 'offers_load'
    ResultsExtract = 'cdp_results_extract'
    Flexible = 'flexible_customer'


def get_tags(owner: Owner, stage: Stage, name: str) -> Dict[str, str]:
    return {
        'cvm-owner': owner.name,
        'cvm-stage': stage.name,
        'cvm-name': name,
    }


def convert_columns_to_upper_case(df: DataFrame):
    for c in df.columns:
        df = df.withColumnRenamed(existing=c, new=c.upper())
    return df


def convert_columns_to_lower_case(df: DataFrame):
    for c in df.columns:
        df = df.withColumnRenamed(existing=c, new=c.lower())
    return df


def nullify_empty_cells(df: DataFrame):
    return df.select(
        [f.when((f.col(c) == "") | (f.col(c) == "NA") | (f.col(c) == "null"), None).otherwise(f.col(c)).alias(c) for c
         in df.columns])


TColumnSpecCustomAttrib = TypeVar("TColumnSpecCustomAttrib")


@dataclass(eq=False)
class ColumnSpec:
    data_type: str
    description: str = ""
    is_partition: bool = False
    customer_profile_name: str = ""  # TODO: Convert to custom attribute
    custom_attributes: list[Any] = field(default_factory=list)

    def __str__(self):
        return self.name

    @property
    def name(self) -> str:
        return self._name

    def __set_name__(self, owner, name):
        self._name = name
        self._table: TableSpec = owner

    def custom(self, t: Type[TColumnSpecCustomAttrib]) -> list[TColumnSpecCustomAttrib]:
        return [ca for ca in self.custom_attributes if type(ca) is t]

    def column_id(self) -> str:
        """
        This method is invoked on elements of TableSpec Enums (i.e. landing.Demographics)

        Sample:
            from cvmdatalake import landing, staging, conformed
            landing.Demographics.idi_turnover.column_id()
        """
        return f"{self._table.table_zone()}.{self._table.table_name()}.{self._name}"

    def column_alias(self) -> str:
        """
        This method is deprecated. Please use ColSpec.alias instead

        :return: column's alias to be used with pyspark col() function
        """
        return self.alias

    @property
    def alias(self) -> str:
        """"
        Sample: landing.Demographics.idi_turnover.alias()
        :return: column's alias to be used with pyspark col() function
        """
        return f"{self._table.table_alias()}.{self._name}"

    @property
    def alias_col(self) -> Column:
        """"
        Sample: landing.Demographics.idi_turnover.alias()
        :return: column's alias to be used with pyspark col() function
        """
        return col(self.alias)


class MetaTableSpec(type):
    def __contains__(cls, x):
        dataset = cast(TableSpec, cls)
        return any(map(lambda field: field.name == x, dataset.fields()))

    def __iter__(cls):
        dataset = cast(TableSpec, cls)
        yield from dataset.fields()

    def __getitem__(cls, item: str) -> ColumnSpec:
        dataset = cast(TableSpec, cls)
        field = cls.__dict__.get(item, None)
        if isinstance(field, ColumnSpec):
            return field

        raise Exception(f"Cannot find field {item} in dataset {dataset.table_id()}")


class TableSpec(metaclass=MetaTableSpec):

    @classmethod
    def fields(cls) -> list[ColumnSpec]:
        return [v for k, v in cls.__dict__.items() if isinstance(v, ColumnSpec)]

    @classmethod
    def class_name(cls) -> str:
        """
        This method is invoked directly on TableSpec Enums

        Sample:
            from cvmdatalake import landing, staging, conformed
            landing.Demographics.class_name()
        """
        return cls.__name__

    @classmethod
    def table_name(cls) -> str:
        """
        This method is invoked directly on TableSpec Enums

        Sample:
            from cvmdatalake import landing, staging, conformed
            landing.Demographics.table_name()
        """
        camel_case_name = cls.__name__
        # covert from CamelCase to snake_case
        return re.sub('([A-Z0-9])', r'_\1', camel_case_name).lower().lstrip('_')

    @classmethod
    def table_description(cls):
        """
        This method is invoked directly on TableSpec Enums. It can be overridden in deriving TableSpec

        Sample:
            from cvmdatalake import landing, staging, conformed
            landing.Demographics.table_description()
        """
        return ""

    @classmethod
    def table_alias(cls) -> str:
        """
        This method is deprecated
        Please use cvmdatalake.TableSpec.alias instead

        :return: table's alias to be used with pyspark dataframe.alias()
        """
        return cls.alias()

    @classmethod
    def alias_df(cls, df: DataFrame) -> DataFrame:
        """
        :return: aliased input DataFrame
        :param df input DataFrame to be alias
        """
        return df.alias(cls.alias())

    @classmethod
    def alias(cls) -> str:
        """
        :return: table's alias to be used with pyspark dataframe.alias()
        """
        return f"{cls.table_zone()}_{cls.table_name()}"

    @classmethod
    def table_zone(cls) -> str:
        """
        This method is invoked directly on TableSpec Enums

        Sample:
            from cvmdatalake import landing, staging, conformed
            landing.Demographics.table_zone()
        """
        packages = cls.__module__.split('.')
        return packages[-min(len(packages), 2)]  # previous to last or the first one if only one package

    @classmethod
    def table_id(cls) -> str:
        """
        This method is invoked directly on TableSpec Enums

        Sample:
            from cvmdatalake import landing, staging, conformed
            landing.Demographics.table_id()
        """
        return f"{cls.table_zone()}.{cls.table_name()}"

    @classmethod
    def catalog_db_name(cls, prefix: str):
        """
        This method is invoked directly on TableSpec Enums

        Sample:
            from cvmdatalake import landing, staging, conformed
            landing.Demographics.catalog_db_name(prefix='dev')
        """
        return f"cvm_{prefix}_{cls.table_zone()}_db"

    @classmethod
    def catalog_table_name(cls, prefix: str):
        """
        This method is invoked directly on TableSpec Enums

        Sample:
            from cvmdatalake import landing, staging, conformed
            landing.Demographics.catalog_table_name(prefix='dev')
        """
        return f"cvm_{prefix}_{cls.table_zone()}_{cls.table_name()}"


class FeatureSpec(TableSpec):
    @classmethod
    def algorithm(cls) -> str:
        pass

    @classmethod
    def feature_type(cls) -> str:
        pass

    @classmethod
    def table_name(cls) -> str:
        result = super().table_name()
        return result.split('_')[0]

    @classmethod
    def table_id(cls) -> str:
        """
        This method is invoked directly on TableSpec Enums

        Sample:
            from cvmdatalake import landing, staging, conformed
            landing.Demographics.table_id()
        """
        return f"{cls.table_zone()}.{cls.algorithm()}.{cls.feature_type().replace('/', '.')}"

    @classmethod
    def catalog_table_name(cls, prefix: str):
        """
        This method is invoked directly on TableSpec Enums

        Sample:
            from cvmdatalake import landing, staging, conformed
            landing.Demographics.catalog_table_name(prefix='dev')
        """
        return f"cvm_{prefix}_{cls.table_zone()}_{cls.algorithm()}_{cls.feature_type().replace('/', '_')}"

    @classmethod
    def table_alias(cls) -> str:
        return f"{cls.table_zone()}_{cls.table_name()}_{cls.feature_type().replace('/', '_')}"

    @classmethod
    def alias(cls) -> str:
        return cls.table_alias()


def get_s3_path(table: Type[TableSpec], lake_descriptor: str) -> str:
    descriptor = json.loads(lake_descriptor)
    return descriptor[table.table_id()]


def get_s3_path_for_feature(table: Type[FeatureSpec], lake_descriptor: str, context=DataFilterContext.default):
    descriptor = json.loads(lake_descriptor)
    path = descriptor[table.table_id()]
    path = path.replace(table.algorithm(), 'features/' + table.algorithm())
    return path + '/' + table.feature_type() + '/' + context


def creat_delta_table_if_not_exists_from_path(spark: SparkSession, table: Type[TableSpec], s3_path: str):
    from delta import DeltaTable
    delta_table = DeltaTable.createIfNotExists(spark).location(s3_path)
    partitioning_cols = []

    for column in table.fields():
        delta_table = delta_table.addColumn(colName=column.name, dataType=column.data_type)

        if column.is_partition:
            partitioning_cols.append(column.name)

    delta_table = delta_table.partitionedBy(*partitioning_cols)
    delta_table.execute()


def creat_delta_table_if_not_exists(spark: SparkSession, table: Type[TableSpec], lake_descriptor: str):
    s3_path = get_s3_path(table, lake_descriptor)
    creat_delta_table_if_not_exists_from_path(spark, table, s3_path)


def create_delta_table_from_catalog(spark: SparkSession, table: Type[TableSpec], lake_descriptor: str):
    creat_delta_table_if_not_exists(spark, table, lake_descriptor)
    s3path = get_s3_path(table, lake_descriptor)
    from delta import DeltaTable
    return DeltaTable.forPath(spark, s3path)


def create_dynamic_frame_from_catalog(
        glue_context, table: Type[TableSpec], prefix: str,
        transformation_ctx: Optional[str] = None,
) -> DynamicFrame:
    if transformation_ctx is None:
        return glue_context.create_dynamic_frame.from_catalog(
            database=table.catalog_db_name(prefix=prefix),
            table_name=table.catalog_table_name(prefix=prefix),
        )
    else:
        return glue_context.create_dynamic_frame.from_catalog(
            database=table.catalog_db_name(prefix=prefix),
            table_name=table.catalog_table_name(prefix=prefix),
            transformation_ctx=transformation_ctx
        )


def landing_to_staging_full_load(glueContext, lnd_table: Type[TableSpec], stg_table: Type[TableSpec],
                                 on_primary_key: str, prefix: str, lake_descriptor):
    lnd_dataset = create_dynamic_frame_from_catalog(glueContext, table=lnd_table, prefix=prefix).toDF()
    lnd_dataset = convert_columns_to_lower_case(lnd_dataset)
    latest_ingestion_date = lnd_dataset.agg(f.max("ingestion_date").alias("latest_ingestion_date")).collect()[0][
        "latest_ingestion_date"]
    print(f"Ingesting {lnd_table.table_name()} for {latest_ingestion_date}")
    lnd_dataset = lnd_dataset.filter(f.col("ingestion_date") == latest_ingestion_date)
    print(f"There are {lnd_dataset.count()} {lnd_table.table_name()} to ingest")
    stg_dataset = create_delta_table_from_catalog(glueContext.spark_session, stg_table, lake_descriptor)
    lnd_dataset = lnd_dataset.drop("ingestion_date")
    lnd_dataset: DataFrame = lnd_dataset.dropDuplicates(
        [
            f"{getattr(lnd_table, on_primary_key).name}"
        ]
    )
    stg_dataset.alias(stg_table.table_alias()).merge(
        lnd_dataset.alias(lnd_table.table_alias()),
        f"""
        {getattr(lnd_table, on_primary_key).column_alias()} = {getattr(stg_table, on_primary_key).column_alias()}
        """
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()


def landing_to_staging_full_load_overwrite(glueContext, lnd_table: Type[TableSpec], stg_table: Type[TableSpec],
                                           on_primary_key: str, drop_duplicates_on_primary: bool, prefix: str,
                                           lake_descriptor):
    lnd_dataset = create_dynamic_frame_from_catalog(glueContext, table=lnd_table, prefix=prefix).toDF()
    lnd_dataset = convert_columns_to_lower_case(lnd_dataset)
    latest_ingestion_date = lnd_dataset.agg(f.max("ingestion_date").alias("latest_ingestion_date")).collect()[0][
        "latest_ingestion_date"]
    print(f"Ingesting {lnd_table.table_name()} for {latest_ingestion_date}")
    lnd_dataset = lnd_dataset.filter(f.col("ingestion_date") == latest_ingestion_date)
    print(f"There are {lnd_dataset.count()} {lnd_table.table_name()} to ingest")
    creat_delta_table_if_not_exists(glueContext.spark_session, stg_table, lake_descriptor)
    lnd_dataset = lnd_dataset.drop("ingestion_date")

    if drop_duplicates_on_primary == True:
        lnd_dataset: DataFrame = lnd_dataset.dropDuplicates(
            [
                f"{getattr(lnd_table, on_primary_key).name}"
            ]
        )
    lnd_dataset.write.format("delta").mode("overwrite").save(get_s3_path(stg_table, lake_descriptor))


def staging_to_conformed_full_load_overwrite(glueContext, stg_table: Type[TableSpec], conf_table: Type[TableSpec],
                                             on_primary_key: str, drop_duplicates_on_primary: bool, lake_descriptor):
    creat_delta_table_if_not_exists(glueContext.spark_session, conf_table, lake_descriptor)
    stg_dataset = create_delta_table_from_catalog(glueContext.spark_session, stg_table, lake_descriptor).toDF()
    if drop_duplicates_on_primary == True:
        stg_dataset: DataFrame = stg_dataset.dropDuplicates(
            [
                f"{getattr(stg_table, on_primary_key).name}"
            ]
        )
    stg_dataset.write.format("delta").mode("overwrite").save(get_s3_path(conf_table, lake_descriptor))


def staging_to_conformed_full_load(glueContext, stg_table: Type[TableSpec], conf_table: Type[TableSpec],
                                   on_primary_key: str, lake_descriptor):
    conf_dataset = create_delta_table_from_catalog(glueContext.spark_session, conf_table, lake_descriptor)
    stg_dataset = create_delta_table_from_catalog(glueContext.spark_session, stg_table, lake_descriptor).toDF()
    stg_dataset: DataFrame = stg_dataset.dropDuplicates(
        [
            f"{getattr(stg_table, on_primary_key).name}"
        ]
    )
    conf_dataset.alias(conf_table.table_alias()).merge(
        stg_dataset.alias(stg_table.table_alias()),
        f"""
        {getattr(stg_table, on_primary_key).column_alias()} = {getattr(conf_table, on_primary_key).column_alias()}
        """
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()


# Define a function to upsert data using merge
def upsert_to_deltatable(spark: SparkSession, deltaTable, column_id, id_name_value, column_to_change, value):
    changes = spark.createDataFrame([(id_name_value, value)], [column_id, column_to_change])
    condition = "original." + column_id + " = changes." + column_id
    update_map = {column_to_change: "changes." + column_to_change}
    insert_map = {column_id: "changes." + column_id, column_to_change: "changes." + column_to_change}
    try:
        # Attempt to run MERGE
        deltaTable.alias("original") \
            .merge(changes.alias("changes"), condition) \
            .whenMatchedUpdate(set=update_map) \
            .whenNotMatchedInsert(values=insert_map) \
            .execute()
    except Exception as e:
        if "concurrency-control" in str(e):
            print("ConcurrentTransactionException, try again.")
            upsert_to_deltatable(spark, deltaTable, column_id, id_name_value, column_to_change, value)
        else:
            return "error"
    return "ok"


def find_in_sequence(sequence: Sequence[str], search: str) -> str:
    for i in range(0, len(sequence)):
        if sequence[i].find(search) != -1:
            return sequence[i]
    return "-1"
