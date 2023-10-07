from typing import List, Type

import pulumi_aws as aws
from pulumi import Output, Input, ResourceOptions
from pulumi_aws.glue import CatalogTableStorageDescriptorColumnArgs, CatalogTablePartitionKeyArgs

from cvmdatalake import FeatureSpec
from cvmdatalake.pulumi import ZoneDescriptor


class CvmFeaturesZone:
    def __init__(self, prefix: str, conformed_bucket_id: Input[str]):
        self.prefix = prefix

        self.tables: List[aws.glue.CatalogTable] = []

        self.features_bucket_id = conformed_bucket_id
        self.features_db = self._cvm_features_db()

        self.lake_descriptor: dict[str, Output[str]] = {}
        self.tables_by_spec: dict[str, Input[str]] = {}

    def _cvm_features_db(self):
        return aws.glue.CatalogDatabase(f"cvm_{self.prefix}_features_db", aws.glue.CatalogDatabaseArgs(
            name=f"cvm_{self.prefix}_features_db"  # Valid names only contain alphabet characters, numbers and _
        ))

    def cvm_feature_table_from_spec(self, table: Type[FeatureSpec]):
        partitions = []
        columns = []

        for column in table.fields():
            if column.is_partition:
                partitions.append(CatalogTablePartitionKeyArgs(
                    name=column.name,
                    type=column.data_type,
                ))
            else:
                columns.append(CatalogTableStorageDescriptorColumnArgs(
                    name=column.name,
                    type=column.data_type,
                ))

        path_prefix = f"{table.algorithm()}/{table.feature_type()}"
        tbl_name_prefix = path_prefix.replace('/', '_')
        self.cvm_feature_table(
            tbl_name=table.table_name(),
            description=table.table_description(),
            partitions=partitions,
            columns=columns,
            path_prefix=path_prefix,
            tbl_name_prefix=tbl_name_prefix
        )

        self.lake_descriptor[table.table_id()] = self.features_bucket_id.apply(
            lambda x: f"s3://{x}/features/{path_prefix}/"
        )

        self.tables_by_spec[table.table_id()] = self.features_bucket_id.apply(
            lambda x: f"s3://{x}/{table.table_name()}"
        )

    def get_zone_descriptor(self) -> ZoneDescriptor:
        return ZoneDescriptor(
            crawlers={},
            tables=self.tables_by_spec
        )

    def cvm_feature_table(
            self, tbl_name: Input[str], description: Input[str], path_prefix: Input[str], tbl_name_prefix: Input[str],
            columns: Input[List[CatalogTableStorageDescriptorColumnArgs]],
            partitions: Input[List[CatalogTablePartitionKeyArgs]] = None
    ) -> aws.glue.CatalogTable:
        # Valid names only contain alphabet characters, numbers and _
        full_table_name = f"cvm_{self.prefix}_features_{tbl_name_prefix}"
        tbl_location = self.features_bucket_id.apply(lambda x: f"s3://{x}/features/{path_prefix}/")

        table = aws.glue.CatalogTable(f"{full_table_name}_table", aws.glue.CatalogTableArgs(
            database_name=self.features_db.name,
            name=full_table_name,
            description=description,
            storage_descriptor=aws.glue.CatalogTableStorageDescriptorArgs(
                columns=columns,
                location=tbl_location,
                input_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                output_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                ser_de_info=aws.glue.CatalogTableStorageDescriptorSerDeInfoArgs(
                    serialization_library="org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                )
            ),
            partition_keys=partitions,
            parameters={
                "EXTERNAL": "TRUE",
            },
        ), opts=ResourceOptions(depends_on=[self.features_db]))

        self.tables.append(table)
        return table
