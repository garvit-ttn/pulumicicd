import json
import os
from typing import Optional, Any

import pulumi
import pulumi_aws as aws
import requests
from pulumi import Input, Output, StackReference
from pulumi_aws import sfn
from pulumi_aws.iam import Role, AwaitableGetRoleResult
from pulumi_aws.sfn import StateMachine
from pulumi_command import local

from cvmdatalake import get_tags, Owner, Stage
from cvmdatalake.data_filters import DataFilterContext
from cvmdatalake.pulumi import transform_descriptor, migrate_to_terraform_opts
from cvmdatalake.workflows import WorkflowGenerator


def add_glue_job_dep(resource_name: str, package_name: str) -> str:
    lib_path = f'dependencies/{environment}/{package_name}'
    aws.s3.BucketObject(
        f"cvm_{environment}_{resource_name}", aws.s3.BucketObjectArgs(
            bucket=misc_bucket.id,
            key=lib_path,
            source=pulumi.FileAsset(f'./dependencies/{package_name}')
        ))

    return lib_path


config = pulumi.Config()
misc_bucket_name = config.get("misc_bucket_name")
environment = config.require("environment")

if misc_bucket_name is not None:
    misc_bucket = aws.s3.get_bucket(misc_bucket_name)
else:
    misc_bucket = aws.s3.Bucket(f"cvm-misc", aws.s3.BucketArgs(
        acl="private",
        versioning=aws.s3.BucketVersioningArgs(enabled=True),
        server_side_encryption_configuration=aws.s3.BucketServerSideEncryptionConfigurationArgs(
            rule=aws.s3.BucketServerSideEncryptionConfigurationRuleArgs(
                apply_server_side_encryption_by_default=aws.s3.BucketServerSideEncryptionConfigurationRuleApplyServerSideEncryptionByDefaultArgs(
                    sse_algorithm='AES256'
                )
            )
        ),
        tags={
            "owner": "exsell",
            "stage": 'misc',
        }
    ), opts=migrate_to_terraform_opts(environment))

sagemaker_featurestore_lib_path = 'dependencies/sagemaker-feature-store-spark-sdk.jar'
spark_feature_store_lib = aws.s3.BucketObject(f"cvm_spark_featurestore_lib", aws.s3.BucketObjectArgs(
    bucket=misc_bucket.id,
    key=sagemaker_featurestore_lib_path,
    source=pulumi.FileAsset(sagemaker_featurestore_lib_path)
))

# it executes command on every preview or up, before any changes are applied.
# there is no need to set up dependencies explicitly
local.run(
    command='cd ../shared/src && python -m build --no-isolation --wheel',
    environment={
        'PYTHONDONTWRITEBYTECODE': "0"
    }
)

cvm_shared_package_name = 'cvm_shared-1.0-py3-none-any.whl'
cvm_shared_lib_path = f'dependencies/{environment}/{cvm_shared_package_name}'
cvm_shared_lib = aws.s3.BucketObject(f"cvm_{environment}_glue_shared_lib", aws.s3.BucketObjectArgs(
    bucket=misc_bucket.id,
    key=cvm_shared_lib_path,
    source=pulumi.FileAsset(f'../shared/src/dist/{cvm_shared_package_name}')
))

cvm_delta_spark_lib_path = add_glue_job_dep(
    'delta_spark_lib',
    'delta_spark-2.2.0-py3-none-any.whl'
)

cvm_delta_spark_import_metadata_lib_path = add_glue_job_dep(
    'delta_spark_import_metadata_lib',
    'importlib_metadata-6.0.0-py3-none-any.whl'
)

cvm_delta_spark_zipp_lib_path = add_glue_job_dep(
    'delta_spark_zipp_lib',
    'zipp-3.14.0-py3-none-any.whl'
)

sqlglot_lib_path = add_glue_job_dep(
    'sqlglot',
    'sqlglot-16.3.1-py3-none-any.whl'
)

faker_lib_path = add_glue_job_dep(
    'faker',
    'Faker-19.2.0-py3-none-any.whl'
)

six_lib_path = add_glue_job_dep(
    'six',
    'six-1.16.0-py2.py3-none-any.whl'
)

# python_dateutil_lib_path = add_glue_job_dep(
#     'python-dateutil',
#     'python_dateutil-2.8.2-py2.py3-none-any.whl'
# )
#
# opensearch_lib_path = add_glue_job_dep(
#     'opensearch-py',
#     'opensearch_py-2.3.0-py2.py3-none-any.whl'
# )
#
# certify_lib_path = add_glue_job_dep(
#     'certifi',
#     'certifi-2023.7.22-py3-none-any.whl'
# )
#
# charset_normalizer_lib_path = add_glue_job_dep(
#     'charset-normalizer',
#     'charset_normalizer-3.2.0-py3-none-any.whl'
# )
#
# idna_lib_path = add_glue_job_dep(
#     'idna',
#     'idna-3.4-py3-none-any.whl'
# )
#
# urllib3_lib_path = add_glue_job_dep(
#     'urllib3',
#     'urllib3-2.0.4-py3-none-any.whl'
# )
#
# requests_lib_path = add_glue_job_dep(
#     'requests',
#     'requests-2.31.0-py3-none-any.whl'
# )

if not os.path.exists('./dependencies/pyspark-3.3.2.tar.gz'):
    print('Downloading pyspark-3.3.2.tar.gz dependency')
    r = requests.get(
        'https://files.pythonhosted.org/packages/6d/08/87b404b8b3255d46caf0ecdccf871d501a2b58da9b844d3f9710ce9d4d53/pyspark-3.3.2.tar.gz')
    with open('./dependencies/pyspark-3.3.2.tar.gz', 'wb') as f:
        f.write(r.content)
    print('Downloading pyspark-3.3.2.tar.gz completed')

cvm_delta_spark_pyspark_lib_path = add_glue_job_dep(
    'delta_spark_pyspark_lib',
    'pyspark-3.3.2.tar.gz'
)

cvm_delta_spark_py4j_lib_path = add_glue_job_dep(
    'delta_spark_py4j_lib',
    'py4j-0.10.9.5-py2.py3-none-any.whl'
)


def cvm_glue_role(prefix: str) -> AwaitableGetRoleResult | Role:
    glue_execution_role_name = config.get("glue_execution_role_name")

    if glue_execution_role_name is not None:
        return aws.iam.get_role(glue_execution_role_name)

    assume_policy = aws.iam.get_policy_document(statements=[aws.iam.GetPolicyDocumentStatementArgs(
        actions=["sts:AssumeRole"],
        effect="Allow",
        principals=[
            aws.iam.GetPolicyDocumentStatementPrincipalArgs(
                type="Service",
                identifiers=["glue.amazonaws.com"],
            ),
            aws.iam.GetPolicyDocumentStatementPrincipalArgs(
                type="Service",
                identifiers=["dms.amazonaws.com"],
            ),
            aws.iam.GetPolicyDocumentStatementPrincipalArgs(
                type="AWS",
                identifiers=["arn:aws:iam::724711057192:root"]
            )
        ],
    )])

    role = aws.iam.Role(
        f"cvm_{prefix}_glue_execution_role",
        path="/",
        assume_role_policy=assume_policy.json,
        opts=migrate_to_terraform_opts(prefix)
    )

    # Here's where we should specify fine-grained actions that airflow need while accessing other AWS services
    permissions = aws.iam.get_policy_document(statements=[

        aws.iam.GetPolicyDocumentStatementArgs(
            sid="AllowBasicOperations",
            effect="Allow",
            resources=["*"],
            actions=[
                "glue:*",
                "lakeformation:*",
                "s3:GetBucketLocation",
                "s3:ListBucket",
                "s3:ListAllMyBuckets",
                "s3:GetBucketAcl",
                "ec2:DescribeVpcEndpoints",
                "ec2:DescribeRouteTables",
                "ec2:CreateNetworkInterface",
                "ec2:DeleteNetworkInterface",
                "ec2:DescribeNetworkInterfaces",
                "ec2:DescribeSecurityGroups",
                "ec2:DescribeSubnets",
                "ec2:DescribeVpcAttribute",
                "iam:ListRolePolicies",
                "iam:GetRole",
                "iam:GetRolePolicy",
                "cloudwatch:PutMetricData"
            ]
        ),
        aws.iam.GetPolicyDocumentStatementArgs(
            effect="Allow",
            resources=["arn:aws:s3:::aws-glue-*"],
            actions=[
                "s3:CreateBucket",
                "s3:PutBucketPublicAccessBlock"
            ],
        ),
        aws.iam.GetPolicyDocumentStatementArgs(
            effect="Allow",
            resources=[
                "arn:aws:s3:::aws-glue-*/*",
                "arn:aws:s3:::*/*aws-glue-*/*"
            ],
            actions=[
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject"
            ],
        ),
        aws.iam.GetPolicyDocumentStatementArgs(
            sid="ListObjectsInDataLakeBuckets",
            effect="Allow",
            resources=[
                "arn:aws:s3:::cvm-*",
            ],
            actions=[
                "s3:ListBucket",
            ],
        ),
        aws.iam.GetPolicyDocumentStatementArgs(
            sid="AllObjectActionsOnDataLakeBuckets",
            effect="Allow",
            resources=[
                "arn:aws:s3:::cvm-*/*",
            ],
            actions=[
                "s3:*Object",
            ],
        ),
        aws.iam.GetPolicyDocumentStatementArgs(
            sid="AllowReadFromCrawlerAndLogsBuckets",
            effect="Allow",
            resources=[
                "arn:aws:s3:::crawler-public*",
                "arn:aws:s3:::aws-glue-*"
            ],
            actions=[
                "s3:GetObject"
            ],
        ),
        aws.iam.GetPolicyDocumentStatementArgs(
            sid="AllowCloudLogAccess",
            effect="Allow",
            resources=[
                "arn:aws:logs:*:*:/aws-glue/*"
            ],
            actions=[
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents",
                "logs:AssociateKmsKey",
            ],
        ),
        aws.iam.GetPolicyDocumentStatementArgs(
            sid="VisualEditor0",
            effect="Allow",
            resources=["*"],
            actions=[
                "glue:UseGlueStudio",
                "iam:ListRoles",
                "iam:ListUsers",
                "iam:ListGroups",
                "iam:ListRolePolicies",
                "iam:GetRole",
                "iam:GetRolePolicy",
                "glue:SearchTables",
                "glue:GetConnections",
                "glue:GetJobs",
                "glue:GetTables",
                "glue:BatchStopJobRun",
                "glue:GetSecurityConfigurations",
                "glue:DeleteJob",
                "glue:GetDatabases",
                "glue:CreateConnection",
                "glue:GetSchema",
                "glue:GetTable",
                "glue:GetMapping",
                "glue:CreateJob",
                "glue:DeleteConnection",
                "glue:CreateScript",
                "glue:UpdateConnection",
                "glue:GetConnection",
                "glue:StartJobRun",
                "glue:GetJobRun",
                "glue:UpdateJob",
                "glue:GetPlan",
                "glue:GetJobRuns",
                "glue:GetTags",
                "glue:GetJob",
                "glue:StartNotebook",
                "glue:TerminateNotebook",
                "glue:GlueNotebookRefreshCredentials",
                "glue:DeregisterDataPreview",
                "glue:GetNotebookInstanceStatus",
                "glue:GlueNotebookAuthorize"
            ],
        ),
        aws.iam.GetPolicyDocumentStatementArgs(
            sid="AllowPassRoleToGlue",
            effect="Allow",
            resources=[
                "arn:aws:iam::*:role/cvm_*_glue_execution_role*"
            ],
            actions=[
                "iam:PassRole"
            ],
            conditions=[
                aws.iam.GetPolicyDocumentStatementConditionArgs(
                    test="ForAllValues:StringEquals",
                    variable="iam:PassedToService",
                    values=["glue.amazonaws.com"]
                )
            ]
        ),
        aws.iam.GetPolicyDocumentStatementArgs(
            sid="AllowPassRole",
            effect="Allow",
            resources=[
                "arn:aws:iam::*:role/cvm_*_glue_execution_role*"
            ],
            actions=[
                "iam:PassRole"
            ],
        ),
        aws.iam.GetPolicyDocumentStatementArgs(
            sid="AllowDynamoDBsRole",
            effect="Allow",
            resources=[
                "*"
            ],
            actions=[
                'dynamodb:*'
            ],
        ),
        aws.iam.GetPolicyDocumentStatementArgs(
            sid="AllowNetworkEC2ResourcesTagging",
            effect="Allow",
            resources=[
                "arn:aws:ec2:*:*:network-interface/*",
                "arn:aws:ec2:*:*:security-group/*",
                "arn:aws:ec2:*:*:instance/*",
            ],
            actions=[
                "ec2:CreateTags",
                "ec2:DeleteTags"
            ],
            conditions=[
                aws.iam.GetPolicyDocumentStatementConditionArgs(
                    test="ForAllValues:StringEquals",
                    variable="aws:TagKeys",
                    values=["aws-glue-service-resource"]
                )
            ]
        ),
        aws.iam.GetPolicyDocumentStatementArgs(
            sid="AllowGetSecretValue",
            effect="Allow",
            resources=["*"],
            actions=[
                "secretsmanager:GetSecretValue",
            ],
        ),
    ])

    aws.iam.RolePolicy(
        f"cvm_{prefix}_glue_execution_policy",
        role=role.id,
        policy=permissions.json,
        opts=migrate_to_terraform_opts(prefix)
    )

    return role


def cvm_workflow_role(prefix: str) -> AwaitableGetRoleResult | Role:
    workflow_execution_role_name = config.get("workflow_execution_role_name")

    if workflow_execution_role_name is not None:
        return aws.iam.get_role(workflow_execution_role_name)

    assume_policy = aws.iam.get_policy_document(statements=[aws.iam.GetPolicyDocumentStatementArgs(
        actions=["sts:AssumeRole"],
        effect="Allow",
        principals=[aws.iam.GetPolicyDocumentStatementPrincipalArgs(
            type="Service",
            identifiers=["states.amazonaws.com", "scheduler.amazonaws.com"],
        )],
    )])

    role = aws.iam.Role(
        f"cvm_{prefix}_workflow_execution_role",
        path="/",
        assume_role_policy=assume_policy.json,
        opts=migrate_to_terraform_opts(prefix)
    )

    permissions = aws.iam.get_policy_document(statements=[
        aws.iam.GetPolicyDocumentStatementArgs(
            effect="Allow",
            resources=["*"],
            actions=[
                "sagemaker:CreateTransformJob",
                "sagemaker:DescribeTransformJob",
                "sagemaker:StopTransformJob",
                "sagemaker:CreateTrainingJob",
                "sagemaker:DescribeTrainingJob",
                "sagemaker:StopTrainingJob",
                "sagemaker:CreateHyperParameterTuningJob",
                "sagemaker:DescribeHyperParameterTuningJob",
                "sagemaker:StopHyperParameterTuningJob",
                "sagemaker:CreateModel",
                "sagemaker:CreateEndpointConfig",
                "sagemaker:CreateEndpoint",
                "sagemaker:DeleteEndpointConfig",
                "sagemaker:DeleteEndpoint",
                "sagemaker:UpdateEndpoint",
                "sagemaker:ListTags",
                "lambda:InvokeFunction",
                "sqs:SendMessage",
                "sns:Publish",
                "ecs:RunTask",
                "ecs:StopTask",
                "ecs:DescribeTasks",
                "dynamodb:GetItem",
                "dynamodb:PutItem",
                "dynamodb:UpdateItem",
                "dynamodb:DeleteItem",
                "batch:SubmitJob",
                "batch:DescribeJobs",
                "batch:TerminateJob",
                "glue:StartJobRun",
                "glue:GetJobRun",
                "glue:GetJobRuns",
                "glue:BatchStopJobRun",
                "glue:StartCrawler",
                "glue:GetCrawler",
                "states:*",
            ]
        ),
        aws.iam.GetPolicyDocumentStatementArgs(
            effect="Allow",
            resources=["*"],
            actions=["iam:PassRole"],
            conditions=[
                aws.iam.GetPolicyDocumentStatementConditionArgs(
                    test="StringEquals",
                    variable="iam:PassedToService",
                    values=["sagemaker.amazonaws.com"],
                ),
            ]
        ),
        aws.iam.GetPolicyDocumentStatementArgs(
            effect="Allow",
            resources=[
                "arn:aws:events:*:*:rule/StepFunctionsGetEventsForSageMakerTrainingJobsRule",
                "arn:aws:events:*:*:rule/StepFunctionsGetEventsForSageMakerTransformJobsRule",
                "arn:aws:events:*:*:rule/StepFunctionsGetEventsForSageMakerTuningJobsRule",
                "arn:aws:events:*:*:rule/StepFunctionsGetEventsForECSTaskRule",
                "arn:aws:events:*:*:rule/StepFunctionsGetEventsForBatchJobsRule",
                "arn:aws:events:*:*:rule/StepFunctionsGetEventsForStepFunctionsExecutionRule",
            ],
            actions=[
                "events:PutTargets",
                "events:PutRule",
                "events:DescribeRule"
            ],
        ),
    ])

    aws.iam.RolePolicy(
        f"cvm_{prefix}_workflow_execution_policy",
        role=role.id,
        policy=permissions.json,
        opts=migrate_to_terraform_opts(prefix)
    )

    return role


def cvm_lambda_role(prefix: str) -> Role:
    assume_policy = aws.iam.get_policy_document(statements=[aws.iam.GetPolicyDocumentStatementArgs(
        actions=["sts:AssumeRole"],
        effect="Allow",
        principals=[aws.iam.GetPolicyDocumentStatementPrincipalArgs(
            type="Service",
            identifiers=["states.amazonaws.com", "scheduler.amazonaws.com", "lambda.amazonaws.com"],
        )],
    )])

    role = aws.iam.Role(
        f"cvm_{prefix}_lambda_execution_role",
        path="/",
        assume_role_policy=assume_policy.json
    )

    permissions = aws.iam.get_policy_document(statements=[
        aws.iam.GetPolicyDocumentStatementArgs(
            effect="Allow",
            resources=["*"],
            actions=[
                "sagemaker:*",
                "lambda:*",
                "states:*",
            ]
        )
    ])

    aws.iam.RolePolicy(
        f"cvm_{prefix}_lambda_execution_policy",
        role=role.id,
        policy=permissions.json
    )

    return role


config = pulumi.Config()
environment = config.require("environment")

glue_exec_role = cvm_glue_role(prefix=environment)
workflow_exec_role = cvm_workflow_role(prefix=environment)
lambda_exec_role = cvm_lambda_role(prefix=environment)


def cvm_glue_job(
        job_path: str, description: str, stage: Stage, lake_stack: StackReference, base_stack: StackReference,
        prefix: str, enable_bookmarks: bool = False, number_of_workers: int = 5, worker_type: str = "G.1X",
        connection: Optional[Output[str]] = None, max_concurrent_runs: int = 10,
        additional_job_args: dict[str, Any] = None
) -> aws.glue.Job:
    assert len(description) > 0, "CVM Glue job description cannot be empty. Use you imagination"

    job_name = job_path[:max([idx for idx, x in enumerate(job_path) if x == '.'])]  # remove file extension
    job_name = job_name.replace('/', '_')

    jobs_base_dir = f"aws-glue-jobs-{prefix}"

    code_s3_path = aws.s3.BucketObject(f"cmv_{prefix}_{job_name}_code", aws.s3.BucketObjectArgs(
        bucket=misc_bucket.id,
        key=f"{jobs_base_dir}/{job_path}",
        source=pulumi.FileAsset(job_path)
    ))

    descriptor = lake_stack.get_output('lake_descriptor').apply(transform_descriptor)
    descriptor = descriptor.apply(lambda d: json.dumps(dict(sorted(d.tables.items()))))

    misc_bucket_id = Output.from_input(misc_bucket.id)

    default_arguments = {
        "--job-bookmark-option": "job-bookmark-enable" if enable_bookmarks else "job-bookmark-disable",
        "--lake_descriptor": descriptor,
        "--rds_cvm_connection_name": connection,
        "--job-language": "python",
        "--data_filter_context": DataFilterContext.default,
        "--cvm_environment": prefix,
        "--enable-spark-ui": "false",
        "--datalake-formats": "delta",
        "--enable-glue-datacatalog": "true",
        "--enable-auto-scaling": "true",
        "--enable-continuous-cloudwatch-log": "true",
        "--enable-metrics": "true",
        "--enable-job-insights": "true",
        "--conf": "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog --conf spark.sql.legacy.parquet.datetimeRebaseModeInRead=LEGACY --conf spark.sql.legacy.parquet.datetimeRebaseModeInWrite=LEGACY --conf spark.sql.parquet.int96RebaseModeInRead=LEGACY --conf spark.sql.parquet.int96RebaseModeInWrite=LEGACY",
        "--additional-python-modules": misc_bucket_id.apply(
            lambda x: ",".join([
                f"s3://{x}/{cvm_shared_lib_path}",
                f"s3://{x}/{cvm_delta_spark_py4j_lib_path}",
                f"s3://{x}/{cvm_delta_spark_pyspark_lib_path}",
                f"s3://{x}/{cvm_delta_spark_zipp_lib_path}",
                f"s3://{x}/{cvm_delta_spark_import_metadata_lib_path}",
                f"s3://{x}/{cvm_delta_spark_lib_path}",
                f"s3://{x}/{sqlglot_lib_path}",
                f"s3://{x}/{faker_lib_path}",
                f"s3://{x}/{six_lib_path}"
                # f"s3://{x}/{python_dateutil_lib_path}",
                # f"s3://{x}/{certify_lib_path}",
                # f"s3://{x}/{opensearch_lib_path}",
                # f"s3://{x}/{charset_normalizer_lib_path}",
                # f"s3://{x}/{idna_lib_path}",
                # f"s3://{x}/{urllib3_lib_path}",
                # f"s3://{x}/{requests_lib_path}",
            ])
        ),
    }

    if additional_job_args is not None:
        default_arguments = {
            **default_arguments,
            **{f"--{k}": v for k, v in additional_job_args.items()},
        }

    job_name = f"cvm_{prefix}_{job_name}_glue_job"
    return aws.glue.Job(
        job_name,
        aws.glue.JobArgs(
            role_arn=glue_exec_role.arn,
            glue_version="4.0",
            number_of_workers=number_of_workers,  # change to autoscaling
            worker_type=worker_type,
            description=description,
            connections=[connection] if connection is not None else connection,
            command=aws.glue.JobCommandArgs(
                script_location=misc_bucket_id.apply(lambda x: f"s3://{x}/{jobs_base_dir}/{job_path}"),
                python_version="3"
            ),
            execution_property=aws.glue.JobExecutionPropertyArgs(
                max_concurrent_runs=max_concurrent_runs
            ),
            # https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-glue-arguments.html
            default_arguments=default_arguments,
            tags=get_tags(Owner.Exsell, stage, job_name)
        ), opts=pulumi.ResourceOptions(depends_on=[code_s3_path]))


def cvm_lambda_function(script_path: str, prefix: str, runtime: str, main_function_name: str) -> aws.lambda_.Function:
    job_name = script_path[:max([idx for idx, x in enumerate(script_path) if x == '.'])]  # remove file extension
    job_name = job_name.replace('/', '_')

    jobs_base_dir = f"aws-glue-jobs-{prefix}"
    catalog = script_path.split("/")[0]
    filename = script_path.split("/")[1]
    filename_without_extensions = filename.split(".")[0]

    code_s3_path = aws.s3.BucketObject(f"cmv_{prefix}_{job_name}_code", aws.s3.BucketObjectArgs(
        bucket=misc_bucket.id,
        key=f"{jobs_base_dir}/{catalog}/{job_name}.zip",
        source=pulumi.AssetArchive({
            f"{filename}": pulumi.FileAsset(script_path)
        })
    ))

    # these two lines were causing Pulumi errors
    # print(misc_bucket.bucket)
    # print(misc_bucket.id.apply(lambda x: f"s3://{x}/{jobs_base_dir}/{script_path}"))

    return aws.lambda_.Function(
        f"lambda_{job_name}",
        aws.lambda_.FunctionArgs(
            role=lambda_exec_role.arn,
            s3_bucket=misc_bucket.id,
            s3_key=f"{jobs_base_dir}/{catalog}/{job_name}.zip",
            handler=f"{filename_without_extensions}.{main_function_name}",
            runtime=runtime,
            source_code_hash="base64-encoded",
            timeout=180
        ),
        opts=pulumi.ResourceOptions(depends_on=[code_s3_path])
    )


def cvm_workflow(name: str, stage: Stage, definition_gen: Input[WorkflowGenerator]) -> StateMachine:
    """

    :rtype: object
    """
    return sfn.StateMachine(
        name,
        role_arn=workflow_exec_role.arn,
        tags=get_tags(Owner.Exsell, stage, name),
        definition=Output.all(arn=workflow_exec_role.arn, gen=definition_gen).apply(
            lambda args: args['gen'](args['arn'])
        ),
    )


def cvm_lake_stack() -> StackReference:
    return cvm_stack('lake', sanitize=False)


def cvm_stack(prefix: str, sanitize: bool = True) -> StackReference:
    config = pulumi.Config()
    environment = config.require("environment")

    if sanitize and environment not in ['dev', 'uat', 'prod']:
        environment = 'dev'

    stack_ref = pulumi.StackReference(f"cvm-{prefix}-{environment}")
    return stack_ref
