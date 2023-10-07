from typing import List, Sequence, Type, Dict

import pulumi_aws as aws
from pulumi import Output, Input, ResourceOptions
from pulumi_aws.glue import CatalogTableStorageDescriptorColumnArgs, CatalogTablePartitionKeyArgs
from pulumi_aws.iam import Role
from pulumi_aws.s3 import AwaitableGetBucketResult, Bucket

from cvmdatalake import Stage, Owner, get_tags, TableSpec
from cvmdatalake.pulumi import ZoneDescriptor, migrate_to_terraform_opts


class CvmLandingZone:
    def __init__(self, crawler_role: Input[Role], prefix: str, bucket_name: Input[str] = None):
        self.bucket_name = bucket_name
        self.crawler_role = crawler_role
        self.prefix = prefix  # it's equal to environment

        self.landing_bucket = self._cvm_landing_data_bucket()
        self.landing_db = self._cvm_landing_db()

        self.tables: List[aws.glue.CatalogTable] = []
        self.crawlers: List[aws.glue.Crawler] = []

        self.lake_descriptor: Dict[str, Input[str]] = {}
        self.crawlers_by_spec: dict[str, Input[str]] = {}
        self.tables_by_spec: dict[str, Input[str]] = {}

    def _cvm_landing_db(self):
        # Valid names only contain alphabet characters, numbers and _
        return aws.glue.CatalogDatabase(f"cvm_{self.prefix}_landing_db", aws.glue.CatalogDatabaseArgs(
            name=f"cvm_{self.prefix}_landing_db"
        ))

    def _cvm_landing_data_bucket(self) -> AwaitableGetBucketResult | Bucket:
        area = "landing"

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
            tags=get_tags(Owner.Exsell, Stage.Ingestion, bucket_name)
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

    def add_bucket_to_lake_formation(self, area, bucket_arn: Input[str]):
        aws.lakeformation.Resource(f"cvm_{self.prefix}_{area}_lf-resource", aws.lakeformation.ResourceArgs(
            arn=bucket_arn,
            role_arn=None,  # when it's None, default service-linked role is used; permissions to S3 are updated
        ))

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
    def landing_bucket_id(self) -> Output[str]:
        if isinstance(self.landing_bucket, AwaitableGetBucketResult):
            return Output.from_input(self.landing_bucket.id)

        if isinstance(self.landing_bucket, Bucket):
            return self.landing_bucket.id

    def cvm_landing_table_from_spec(self, table: Type[TableSpec]):
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

        tbl, crawler = self.cvm_landing_table(
            tbl_name=table.table_name(),
            description=table.table_description(),
            partitions=partitions,
            columns=columns,
        )

        self.tables_by_spec[table.table_id()] = self.landing_bucket_id.apply(
            lambda x: f"s3://{x}/{table.table_name()}"
        )

        self.crawlers_by_spec[table.table_id()] = crawler.name

        self.lake_descriptor[table.table_id()] = self.landing_bucket_id.apply(
            lambda x: f"s3://{x}/{table.table_name()}"
        )

    def cvm_landing_table(
            self, tbl_name: Input[str], description: Input[str],
            columns: Input[List[CatalogTableStorageDescriptorColumnArgs]],
            partitions: Input[List[CatalogTablePartitionKeyArgs]]
    ) -> tuple[aws.glue.CatalogTable, aws.glue.Crawler]:
        full_table_name = f"cvm_{self.prefix}_landing_{tbl_name}"  # Valid names only contain alphabet characters, numbers and _

        table = aws.glue.CatalogTable(f"{full_table_name}_table", aws.glue.CatalogTableArgs(
            database_name=self.landing_db.name,
            name=full_table_name,
            description=description,
            storage_descriptor=aws.glue.CatalogTableStorageDescriptorArgs(
                columns=columns,
                location=self.landing_bucket_id.apply(lambda x: f"s3://{x}/{tbl_name}"),
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

        ), opts=ResourceOptions(depends_on=[self.landing_db]))

        # Maybe we should create a single crawler that will handle all landing tables at once
        # as they're all in the same GlueDB
        crawler_name = f"{full_table_name}_crawler"
        crawler = aws.glue.Crawler(crawler_name, aws.glue.CrawlerArgs(
            database_name=self.landing_db.name,
            role=self.crawler_role.arn,
            schema_change_policy=aws.glue.CrawlerSchemaChangePolicyArgs(
                delete_behavior="LOG",
                update_behavior="LOG"
            ),
            configuration="""
                {
                  "Version":1.0,
                  "Grouping": {
                    "TableGroupingPolicy": "CombineCompatibleSchemas"
                  }
                }""",
            catalog_targets=[
                aws.glue.CrawlerCatalogTargetArgs(
                    database_name=self.landing_db.name,
                    tables=[full_table_name]
                )
            ],
            tags=get_tags(Owner.Exsell, Stage.Ingestion, crawler_name)
        ), opts=ResourceOptions(depends_on=[self.landing_db, table]))

        # keep the relevant state for later
        self.crawlers.append(crawler)
        self.tables.append(table)

        return table, crawler
