from functools import partial

from pulumi import Input, StackReference, Output
from stepfunctions.steps import Chain, GlueStartJobRunStep, Task, Choice, Wait, ChoiceRule
from stepfunctions.workflow import Workflow

from cvmdatalake.pulumi import transform_descriptor


def generate_workflow_definition_lift(
        feature_preparation_job_name: Input[str],
        lake_stack: StackReference,
        sm_execution_role: Input[str],
) -> Output[partial]:
    return Output.all(
        feature_preparation_job_name=feature_preparation_job_name,
        lake_descriptor=lake_stack.get_output('lake_descriptor').apply(transform_descriptor),
        sm_execution_role=sm_execution_role
    ).apply(
        lambda args: partial(
            generate_workflow_definition,
            sm_execution_role=args['sm_execution_role'],
            lake_descriptor=args['lake_descriptor'],
            feature_preparation_job_name=args['feature_preparation_job_name']
        )
    )


def generate_workflow_definition(
        workflow_execution_role_arn: str, lake_descriptor: str, sm_execution_role: str,
        feature_preparation_job_name: str
) -> str:
    feature_prep_job = GlueStartJobRunStep(
        state_id=f"Run-{feature_preparation_job_name}"[0:80],
        input_path="$",
        result_path="$.result",
        output_path="$",
        wait_for_completion=True,
        parameters={
            'JobName': feature_preparation_job_name,
            'Arguments': {
                "--data_filter_context.$": "$.data_filter.context",
            }
        }
    )

    model_name_create = f"nbom-model-create"

    wait_for_model_package = Wait(state_id="nbom-wait-for-model-package", seconds=30)

    nbom_train_job = training_step(lake_descriptor, sm_execution_role)
    nbom_create_model_package = create_model_package(lake_descriptor, sm_execution_role)
    nbom_describe_model_package = describe_model_package(lake_descriptor, sm_execution_role)
    nbom_choice_model_package_status = choice_model_package_status(model_name_create, nbom_describe_model_package,
                                                                  sm_execution_role)

    flow = Workflow(
        name="cvm-nbom-train-workflow",
        role=workflow_execution_role_arn,
        definition=Chain([
             feature_prep_job,
            # nbom_train_job,
            # nbom_create_model_package,
            # nbom_describe_model_package,
            # wait_for_model_package,
            # nbom_choice_model_package_status,

        ])
    )

    return flow.definition.to_json(pretty=True)


def training_step(lake_descriptor, sm_execution_role):
    return Task(
        state_id='Run-nbom-training',
        input_path="$",
        result_path="$.result",
        output_path="$",
        resource="arn:aws:states:::sagemaker:createTrainingJob.sync",
        parameters={
            "TrainingJobName.$": "$.data_filter.TrainingJobName",
            "EnableManagedSpotTraining": True,
            "EnableNetworkIsolation": True,
            "RoleArn": sm_execution_role,
            "StoppingCondition": {
                "MaxRuntimeInSeconds": 86400,
                "MaxWaitTimeInSeconds": 86400
            },
            "AlgorithmSpecification": {
                "AlgorithmName.$": "$.data_filter.AlgorithmName",
                "EnableSageMakerMetricsTimeSeries": True,
                "TrainingInputMode": "File"
            },
            "HyperParameters.$": "$.hyperparams.values",
            "ResourceConfig": {
                "InstanceCount": 1,
                "InstanceType": "ml.g4dn.16xlarge",
                "VolumeSizeInGB": 200
            },
            "OutputDataConfig": {
                "KmsKeyId.$": "$.data_filter.KmsKeyId",
                "S3OutputPath.$": "$.data_filter.model_path"
            },
            "InputDataConfig": [
                {
                    "ChannelName": "training",
                    "InputMode": "File",
                    "CompressionType": "None",
                    "ContentType": "text/csv",
                    "RecordWrapperType": "None",
                    "DataSource": {
                        "S3DataSource": {
                            "S3DataType": "S3Prefix",
                            "S3Uri.$": "$.data_filter.input_train_path",
                            "S3DataDistributionType": "FullyReplicated",
                            "AttributeNames": [
                                "S3"
                            ]
                        }
                    }
                }
            ]
        }
    )


def create_model_package(lake_descriptor, sm_execution_role):
    return Task(
        state_id='nbom-create-model-package',
        input_path="$",
        result_path="$.result",
        output_path="$",
        resource="arn:aws:states:::aws-sdk:sagemaker:createModelPackage",
        parameters={
            "ModelPackageDescription": "Nbom model package",
            "ModelPackageName.$": "$.data_filter.ModelPackageName",
            "SourceAlgorithmSpecification": {
                "SourceAlgorithms": [
                    {
                        "AlgorithmName.$": "$.data_filter.AlgorithmName",
                        "ModelDataUrl.$": "$.data_filter.ModelDataUrl"
                    }
                ]
            }
        }
    )


def describe_model_package(lake_descriptor, sm_execution_role):
    return Task(
        state_id='nbom-describe-model-package',
        input_path="$",
        result_path="$.result",
        output_path="$",
        resource="arn:aws:states:::aws-sdk:sagemaker:describeModelPackage",
        parameters={
            "ModelPackageName.$": "$.data_filter.ModelPackageName"
        }
    )


def choice_model_package_status(create_model_name, wait_for_model_package, sm_execution_role):
    choice = Choice(
        state_id="nbom-choice-if-model-package-is-ready",
        input_path="$",
        output_path="$",
    )

    choice.add_choice(
        rule=ChoiceRule.StringEquals(variable="$.result.ModelPackageStatus", value="Completed"),
        next_step=Task(
            state_id=create_model_name,
            input_path="$",
            result_path="$.result",
            output_path="$",
            resource="arn:aws:states:::sagemaker:createModel",
            parameters={
                "Containers": [
                    {
                        "ModelPackageName.$": "$.data_filter.ModelPackageName"
                    }
                ],
                "EnableNetworkIsolation": True,
                "ExecutionRoleArn": sm_execution_role,
                "ModelName.$": "$.data_filter.ModelName"
            }))
    choice.add_choice(
        rule=ChoiceRule.Not(ChoiceRule.StringEquals(variable="$.result.ModelPackageStatus", value="Completed")),
        next_step=wait_for_model_package)

    return choice


# {
#     "Comment": "Insert your JSON here",
#     "data_filter": {
#         "context": "nbom.bu.share",
#         "TrainingJobName": "nbom-bu-share-train-2023-03-40",
#         "TransformJobName": "nbom-bu-share-transform-2023-03-40",
#         "AlgorithmName": "arn:aws:sagemaker:eu-west-1:985815980388:algorithm/maf-recommenderx-dask-rc7-781ad608939636b3a8c4ca212885bd19",
#         "ModelPackageName": "nbom-bu-share-model-pacakge-2023-03-40",
#         "ModelName": "nbom-bu-share-model-2023-03-40",
#         "model_path": "s3://cvm-uat-conformed-d5b175d/features/nbom/model/nbom.bu.share/",
#         "output_path": "s3://cvm-uat-conformed-d5b175d/features/nbom/output/nbom.bu.share/",
#         "ModelDataUrl": "s3://cvm-uat-conformed-d5b175d/features/nbom/model/nbom.bu.share/nbom-bu-share-train-2023-03-40/output/model.tar.gz",
#         "input_train_path": "s3://cvm-uat-conformed-d5b175d/features/nbom/input/train/nbom.bu.share/",
#         "input_transform_path": "s3://cvm-uat-conformed-d5b175d/features/nbom/input/transform/nbom.bu.share/",
#         "KmsKeyId": "e65fea64-7da6-4e27-bf4f-9cc2eb54ef0a"
#     },
#     "hyperparams": {
#         "data_filter_context_index": "0",
#         "values": {
#             "epochs": "1",
#             "embedding_dimensions": "32",
#             "number_of_offers": "5",
#             "learning_rate": "0.1"
#         }
#     }
# }