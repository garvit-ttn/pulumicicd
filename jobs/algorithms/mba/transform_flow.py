from functools import partial
from typing import Sequence

from pulumi import Input, StackReference, Output
from stepfunctions.steps import Chain, GlueStartJobRunStep, Task
from stepfunctions.workflow import Workflow

from cvmdatalake import find_in_sequence
from cvmdatalake.pulumi import transform_descriptor


def generate_workflow_definition_lift(
        feature_preparation_job_name: Input[str],
        output_to_predictions_job_name: Input[str],
        lake_stack: StackReference
) -> Output[partial]:
    return Output.all(
        feature_preparation_job_name=feature_preparation_job_name,
        output_to_predictions_job_name=output_to_predictions_job_name,
        lake_descriptor=lake_stack.get_output('lake_descriptor').apply(transform_descriptor),
        conformed_crawlers=lake_stack.get_output('conformed.crawlers')
    ).apply(
        lambda args: partial(
            generate_workflow_definition,
            lake_descriptor=args['lake_descriptor'],
            feature_preparation_job_name=args['feature_preparation_job_name'],
            output_to_predictions_job_name=args['output_to_predictions_job_name'],
            conformed_crawlers=args['conformed_crawlers']
        )
    )


def generate_workflow_definition(
        workflow_execution_role_arn: str, lake_descriptor: str,
        feature_preparation_job_name: str,
        output_to_predictions_job_name: str,
        conformed_crawlers: Sequence[str]
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

    mba_job_name = "mba-inference"

    mba_inference_job = transform_step( mba_job_name)

    output_to_predictions_job = GlueStartJobRunStep(
        state_id=f"Run-{output_to_predictions_job_name}"[0:80],
        input_path="$",
        result_path="$.result",
        output_path="$",
        wait_for_completion=True,
        parameters={
            'JobName': output_to_predictions_job_name,
            'Arguments': {
                "--data_filter_context.$": "$.data_filter.context",
            }
        }
    )

    start_crawler = Task(
        state_id=f"Start-mba-crawler_name",
        resource="arn:aws:states:::aws-sdk:glue:startCrawler",
        parameters={
            'Name': find_in_sequence(conformed_crawlers, "profile_algorithms_output")
        }
    )

    flow = Workflow(
        name="cvm-mba-transform-workflow",
        role=workflow_execution_role_arn,
        definition=Chain([
            feature_prep_job,
            mba_inference_job,
            output_to_predictions_job,
            start_crawler
        ])
    )

    return flow.definition.to_json(pretty=True)


def transform_step(mba_job_name):
    return Task(
        state_id=f"Run-{mba_job_name}"[0:80],
        input_path="$",
        result_path="$.result",
        output_path="$",
        resource="arn:aws:states:::sagemaker:createTransformJob.sync",
        parameters={
            "TransformJobName.$": "$.data_filter.TransformJobName",
            "ModelName.$": "$.data_filter.ModelName",
            "TransformInput": {
                "DataSource": {
                    "S3DataSource": {
                        "S3DataType": "S3Prefix",
                        "S3Uri.$": "$.data_filter.input_transform_path"
                    }
                },
                "ContentType": "text/csv"
            },
            "TransformOutput": {
                "S3OutputPath.$": "$.data_filter.output_path"
            },
            "TransformResources": {
                "InstanceCount": 1,
                "InstanceType": "ml.m5.24xlarge"
            }
        }
    )

# {
#   "Comment": "Insert your JSON here",
#   "data_filter": {
#     "context": "mba.bu.share",
#     "TrainingJobName": "mba-bu-share-train-2023-03-29",
# 	"TransformJobName": "mba-bu-share-transform-2023-03-29",
#     "AlgorithmName": "arn:aws:sagemaker:eu-west-1:985815980388:algorithm/maf-mba-dask-rc4-a784bacc94ce3f7bb8c497a3adffb983",
#     "ModelPackageName": "mba-bu-share-model-pacakge-2023-03-29",
#     "ModelName": "mba-bu-share-model-2023-03-29",
#     "model_path": "s3://cvm-uat-conformed-d5b175d/features/mba/model/mba.bu.share/",
#     "output_path": "s3://cvm-uat-conformed-d5b175d/features/mba/output/mba.bu.share/",
#     "ModelDataUrl": "s3://cvm-uat-conformed-d5b175d/features/mba/model/mba.bu.share/mba-bu-share-train-2023-03-29/output/model.tar.gz",
#     "input_train_path": "s3://cvm-uat-conformed-d5b175d/features/mba/input/train/mba.bu.share/",
# 	"input_transform_path": "s3://cvm-uat-conformed-d5b175d/features/mba/input/transform/mba.bu.share/",
# 	"KmsKeyId": "e65fea64-7da6-4e27-bf4f-9cc2eb54ef0a"
#   },
#   "hyperparams": {
#     "data_filter_context_index": "0",
#     "values": {
#       "penalizer_coef": "0.01",
#       "months_to_predict": "6",
#       "testing_period": "9",
#       "max_correlation_coef": "0.7",
#       "minimum_purchases": "1",
#       "training_period": "11",
#       "discount_rate": "0.01"
#     }
#   }
# }