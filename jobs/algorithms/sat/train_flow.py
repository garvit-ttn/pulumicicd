from functools import partial

from pulumi import Input, StackReference, Output
from stepfunctions.steps import Chain, GlueStartJobRunStep, Task
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
    sat_feature_prep_job = GlueStartJobRunStep(
        state_id=f"Run-{feature_preparation_job_name}"[0:80],
        input_path="$.data_filter",
        result_path="$.result",
        output_path="$",
        wait_for_completion=True,
        parameters={
            'JobName': feature_preparation_job_name,
            'Arguments': {
                "--data_filter_context.$": "$.context",
            }
        }
    )

    # sat_train_job = training_step(lake_descriptor, sm_execution_role)

    flow = Workflow(
        name="cvm-sat-train-workflow",
        role=workflow_execution_role_arn,
        definition=Chain([
            # rfm for customer segment

            # sat
            sat_feature_prep_job,
            # sat_train_job
        ])
    )

    return flow.definition.to_json(pretty=True)


def training_step(lake_descriptor, sm_execution_role):
    return Task(
        state_id='Run-clv-training',
        resource="arn:aws:states:::sagemaker:createTrainingJob.sync",
        parameters={
            "TrainingJobName.$": "States.UUID()",
            "EnableManagedSpotTraining": True,
            "EnableNetworkIsolation": True,
            "RoleArn": sm_execution_role,
            "StoppingCondition": {
                "MaxRuntimeInSeconds": 86400,
                "MaxWaitTimeInSeconds": 86400
            },
            "AlgorithmSpecification": {
                "AlgorithmName": 'arn:aws:sagemaker:eu-west-1:985815980388:algorithm/maf-clv-dask-rc4-a784bacc94ce3f7bb8c497a3adffb983',
                "EnableSageMakerMetricsTimeSeries": True,
                "TrainingInputMode": "File"
            },
            "HyperParameters.$": "$.hyperparams",
            "ResourceConfig": {
                "InstanceCount": 1,
                "InstanceType": "ml.m5.24xlarge",
                "VolumeSizeInGB": 200
            },
            "OutputDataConfig": {
                "KmsKeyId": "e65fea64-7da6-4e27-bf4f-9cc2eb54ef0a",  # TODO: parametrize
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
                            "S3Uri.$": "$.data_filter.input_path",
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

# {
#     "Comment": "Insert your JSON here",
# 	"data_filter": {
# 		"context": "sat_bu_share",
# 		"model_path": "s3://cvm-uat-conformed-d5b175d/features/sat/model/",
# 		"output_path": "s3://cvm-uat-conformed-d5b175d/features/sat/output/"
# 	}
# }
