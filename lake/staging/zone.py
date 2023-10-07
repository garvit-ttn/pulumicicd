from typing import List, Sequence, Type

import pulumi_aws as aws
from pulumi import Output, Input, ResourceOptions
from pulumi_aws.glue import CatalogTableStorageDescriptorColumnArgs, CatalogTablePartitionKeyArgs
from pulumi_aws.iam import Role
from pulumi_aws.s3 import Bucket, AwaitableGetBucketResult

from cvmdatalake import get_tags, Owner, Stage, TableSpec
from cvmdatalake.pulumi import ZoneDescriptor, migrate_to_terraform_opts


class CvmStagingZone:
    def __init__(self, crawler_role: Input[Role], prefix: str, bucket_name: Input[str] = None):
        self.bucket_name = bucket_name
        self.crawler_role = crawler_role
        self.prefix = prefix

        self.tables: List[aws.glue.CatalogTable] = []
        self.crawlers: List[aws.glue.Crawler] = []

        self.staging_bucket = self._cvm_staging_data_bucket()
        self.staging_db = self._cvm_staging_db()

        self.lake_descriptor: dict[str, Input[str]] = {}
        self.crawlers_by_spec: dict[str, Input[str]] = {}
        self.tables_by_spec: dict[str, Input[str]] = {}

    def _cvm_staging_db(self):
        return aws.glue.CatalogDatabase(f"cvm_{self.prefix}_staging_db", aws.glue.CatalogDatabaseArgs(
            name=f"cvm_{self.prefix}_staging_db"  # Valid names only contain alphabet characters, numbers and _
        ))

    def add_bucket_to_lake_formation(self, area, bucket_arn: Input[str]):
        aws.lakeformation.Resource(f"cvm_{self.prefix}_{area}_lf_resource", aws.lakeformation.ResourceArgs(
            arn=bucket_arn,
            role_arn=None  # when it's None, default service-linked role is used; permissions to S3 are updated
        ))

    def _cvm_staging_data_bucket(self) -> AwaitableGetBucketResult | Bucket:
        area = "staging"

        if self.bucket_name is not None:
            bucket = aws.s3.get_bucket(bucket=self.bucket_name)
            self.add_bucket_to_lake_formation(area, bucket.arn)
            return bucket

        bucket_name = f"cvm-{self.prefix}-{area}"
        bucket = aws.s3.Bucket(bucket_name, aws.s3.BucketArgs(
            acl="private",
            versioning=aws.s3.BucketVersioningArgs(enabled=True),
            server_side_encryption_configuration=aws.s3.BucketServerSideEncryptionConfigurationArgs(
                rule=aws.s3.BucketServerSideEncryptionConfigurationRuleArgs(
                    apply_server_side_encryption_by_default=aws.s3.BucketServerSideEncryptionConfigurationRuleApplyServerSideEncryptionByDefaultArgs(
                        sse_algorithm='AES256'
                    )
                )
            ),
            tags=get_tags(Owner.Exsell, Stage.DataQuality, bucket_name)
        ), opts=migrate_to_terraform_opts(self.prefix))

        aws.s3.BucketLifecycleConfigurationV2(
            f"cvm-{self.prefix}-{area}-lifecycle",
            aws.s3.BucketLifecycleConfigurationV2Args(
                bucket=bucket.id,
                rules=[
                    aws.s3.BucketLifecycleConfigurationV2RuleArgs(
                        id=f"cvm-{self.prefix}-{area}-lifecycle-data-archival",
                        status="Enabled",
                        expiration=aws.s3.BucketLifecycleConfigurationV2RuleExpirationArgs(
                            days=7 * 365,
                        ),
                        abort_incomplete_multipart_upload=aws.s3.BucketLifecycleConfigurationV2RuleAbortIncompleteMultipartUploadArgs(
                            days_after_initiation=1
                        ),
                        filter=aws.s3.BucketLifecycleConfigurationV2RuleFilterArgs(),
                        transitions=[
                            aws.s3.BucketLifecycleConfigurationV2RuleTransitionArgs(
                                days=3 * 365,
                                storage_class="STANDARD_IA",
                            ),
                            aws.s3.BucketLifecycleConfigurationV2RuleTransitionArgs(
                                days=4 * 365,
                                storage_class="GLACIER_IR",
                            ),
                            aws.s3.BucketLifecycleConfigurationV2RuleTransitionArgs(
                                days=5 * 365,
                                storage_class="GLACIER",
                            ),
                            aws.s3.BucketLifecycleConfigurationV2RuleTransitionArgs(
                                days=6 * 365,
                                storage_class="DEEP_ARCHIVE",
                            ),
                        ],
                    )
                ]
            ), opts=migrate_to_terraform_opts(self.prefix)
        )

        self.add_bucket_to_lake_formation(area, bucket.arn)
        return bucket

    def get_tables_names(self) -> Sequence[Output[str]]:
        for tbl in self.tables:
            yield tbl.name.apply(lambda x: x)

    def get_crawlers_names(self) -> Sequence[Output[str]]:
        for crl_name in self.crawlers:
            yield crl_name.name.apply(lambda x: x)

    def get_zone_descriptor(self) -> ZoneDescriptor:
        return ZoneDescriptor(
            crawlers=self.crawlers_by_spec,
            tables=self.tables_by_spec
        )

    @property
    def staging_bucket_id(self) -> Output[str]:
        if isinstance(self.staging_bucket, AwaitableGetBucketResult):
            return Output.from_input(self.staging_bucket.id)

        if isinstance(self.staging_bucket, Bucket):
            return self.staging_bucket.id

    def cvm_staging_table_from_spec(self, table: Type[TableSpec]):
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

        tbl, crawler = self.cvm_staging_table(
            tbl_name=table.table_name(),
            description=table.table_description(),
            partitions=partitions,
            columns=columns,
        )

        self.tables_by_spec[table.table_id()] = self.staging_bucket_id.apply(
            lambda x: f"s3://{x}/{table.table_name()}"
        )

        self.crawlers_by_spec[table.table_id()] = crawler.name

        self.lake_descriptor[table.table_id()] = self.staging_bucket_id.apply(
            lambda x: f"s3://{x}/{table.table_name()}"
        )

    def cvm_staging_table(
            self, tbl_name: Input[str], description: Input[str],
            columns: Input[List[CatalogTableStorageDescriptorColumnArgs]],
            partitions: Input[List[CatalogTablePartitionKeyArgs]] = None
    ) -> tuple[aws.glue.CatalogTable, aws.glue.Crawler]:
        # Valid names only contain alphabet characters, numbers and _
        full_table_name = f"cvm_{self.prefix}_staging_{tbl_name}"
        tbl_location = self.staging_bucket_id.apply(lambda x: f"s3://{x}/{tbl_name}/")

        table = aws.glue.CatalogTable(f"{full_table_name}_table", aws.glue.CatalogTableArgs(
            database_name=self.staging_db.name,
            name=full_table_name,
            description=description,
            storage_descriptor=aws.glue.CatalogTableStorageDescriptorArgs(
                columns=columns,
                location=self.staging_bucket_id.apply(lambda x: f"s3://{x}/{tbl_name}/_symlink_format_manifest"),
                input_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                output_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                ser_de_info=aws.glue.CatalogTableStorageDescriptorSerDeInfoArgs(
                    serialization_library="org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                )
            ),
            partition_keys=partitions,
            parameters={
                "classification": "parquet",
                "parquet.compression": "SNAPPY",
                "EXTERNAL": "TRUE",
            },
        ), opts=ResourceOptions(depends_on=[self.staging_db]))

        crawler_name = f"{full_table_name}_crawler"
        crawler = aws.glue.Crawler(crawler_name, aws.glue.CrawlerArgs(
            database_name=self.staging_db.name,
            table_prefix=f"cvm_{self.prefix}_staging_",
            role=self.crawler_role.arn,
            delta_targets=[
                aws.glue.CrawlerDeltaTargetArgs(
                    connection_name="",  # It's intentional. Connection name must be empty
                    write_manifest=True,  # It's required to query Delta tables in Athena
                    delta_tables=[
                        tbl_location,
                    ]
                ),
            ],
            tags=get_tags(Owner.Exsell, Stage.DataQuality, crawler_name)
        ), opts=ResourceOptions(depends_on=[self.staging_db, table]))

        # keep the relevant state for later
        self.crawlers.append(crawler)
        self.tables.append(table)

        return table, crawler
