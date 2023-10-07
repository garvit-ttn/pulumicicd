import pulumi_aws as aws
import pulumi_aws_native as aws_native
from cvmdatalake import get_tags, Owner, Stage


def provision(sagemaker: aws.sagemaker.Domain):
    # aws_native.sagemaker.FeatureGroup('example2', aws_native.sagemaker.FeatureGroupArgs(
    #     feature_group_name="cvm-features-core",
    #     description='CVM core feature group with basic attributes',
    #     record_identifier_feature_name="id",
    #     event_time_feature_name="timestamp",
    #     role_arn=sagemaker.default_user_settings.execution_role,
    #     online_store_config=aws_native.sagemaker.OnlineStoreConfigPropertiesArgs(
    #         enable_online_store=True,
    #     ),
    #     feature_definitions=[
    #         aws_native.sagemaker.FeatureGroupFeatureDefinitionArgs(
    #             feature_name="id",
    #             feature_type=aws_native.sagemaker.FeatureGroupFeatureDefinitionFeatureType(value='String'),
    #         ),
    #         aws_native.sagemaker.FeatureGroupFeatureDefinitionArgs(
    #             feature_name="id",
    #             feature_type=aws_native.sagemaker.FeatureGroupFeatureDefinitionFeatureType(value='String'),
    #         ),
    #         aws_native.sagemaker.FeatureGroupFeatureDefinitionArgs(
    #             feature_name="id",
    #             feature_type=aws_native.sagemaker.FeatureGroupFeatureDefinitionFeatureType(value='String'),
    #         ),
    #         aws_native.sagemaker.FeatureGroupFeatureDefinitionArgs(
    #             feature_name="id",
    #             feature_type=aws_native.sagemaker.FeatureGroupFeatureDefinitionFeatureType(value='String')
    #         )
    #     ],
    # ))

    bucket = aws.s3.Bucket(f"cvm-features", aws.s3.BucketArgs(
        acl="private",
        versioning=aws.s3.BucketVersioningArgs(enabled=True),
        server_side_encryption_configuration=aws.s3.BucketServerSideEncryptionConfigurationArgs(
            rule=aws.s3.BucketServerSideEncryptionConfigurationRuleArgs(
                apply_server_side_encryption_by_default=aws.s3.BucketServerSideEncryptionConfigurationRuleApplyServerSideEncryptionByDefaultArgs(
                    sse_algorithm='AES256'
                )
            )
        ),
        tags=get_tags(Owner.Exsell, Stage.FeaturePrep, "cvm-features")
    ))

    aws.lakeformation.Resource(f"cvm-features-lf-resource", aws.lakeformation.ResourceArgs(
        arn=bucket.arn,
        role_arn=None,  # when it's None, default service-linked role is used; permissions to S3 are updated
    ))

    # features_db = aws.glue.CatalogDatabase(f"cvm_features-db", aws.glue.CatalogDatabaseArgs(
    #     name=f"cvm_features"  # Valid names only contain alphabet characters, numbers and _
    # ))

    # https://github.com/aws/aws-cli/issues/6702
    cvm_features_core = aws.sagemaker.FeatureGroup("cvm-features-core", aws.sagemaker.FeatureGroupArgs(
        feature_group_name="cvm-features-core",
        description='CVM core feature group with basic attributes',
        record_identifier_feature_name="id",
        event_time_feature_name="timestamp",
        role_arn=sagemaker.default_user_settings.execution_role,
        online_store_config=aws.sagemaker.FeatureGroupOnlineStoreConfigArgs(
            enable_online_store=True,
        ),
        offline_store_config=aws.sagemaker.FeatureGroupOfflineStoreConfigArgs(
            s3_storage_config=aws.sagemaker.FeatureGroupOfflineStoreConfigS3StorageConfigArgs(
                s3_uri=bucket.id.apply(lambda x: f's3://{x}/core')
            ),
            # data_catalog_config=aws.sagemaker.FeatureGroupOfflineStoreConfigDataCatalogConfigArgs(
            #     catalog=features_db.catalog_id,
            #     database=features_db.name,
            #     table_name='core'
            # )
        ),
        feature_definitions=[
            aws.sagemaker.FeatureGroupFeatureDefinitionArgs(
                feature_name="id",
                feature_type="String",
            ),
            aws.sagemaker.FeatureGroupFeatureDefinitionArgs(
                feature_name="timestamp",
                feature_type="String",
            ),
            aws.sagemaker.FeatureGroupFeatureDefinitionArgs(
                feature_name="col1",
                feature_type="String",
            ),
            aws.sagemaker.FeatureGroupFeatureDefinitionArgs(
                feature_name="col2",
                feature_type="String",
            ),
            aws.sagemaker.FeatureGroupFeatureDefinitionArgs(
                feature_name="col3",
                feature_type="String",
            )
        ],
    ))
