from functools import partial
from typing import Sequence

from pulumi import Input, StackReference, Output
from stepfunctions.steps import *
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

    retry = Retry(
        error_equals=["States.ALL"],
        interval_seconds=5,
        max_attempts=5,
        backoff_rate=2,
    )

    pass_crawler = Pass(state_id=f"Pass-nbo-crawler_name")

    crawler_catch = Catch(
        error_equals=["States.ALL"],
        next_step=pass_crawler
    )

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

    nbo_job_name = "nbo-inference"

    nbo_inference_job = transform_step(nbo_job_name, retry)

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
        },
        retry=retry
    )

    start_crawler = Task(
        state_id=f"Start-nbo-crawler_name",
        resource="arn:aws:states:::aws-sdk:glue:startCrawler",
        parameters={
            'Name': find_in_sequence(conformed_crawlers, "profile_algorithms_output")
        },
        retry=retry,
        catch=crawler_catch
    )

    flow = Workflow(
        name="cvm-nbo-transform-workflow",
        role=workflow_execution_role_arn,
        definition=Chain([
            feature_prep_job,
            nbo_inference_job,
            output_to_predictions_job,
            start_crawler
        ])
    )

    return flow.definition.to_json(pretty=True)


def transform_step(nbo_job_name, retry):
    return Task(
        state_id=f"Run-{nbo_job_name}"[0:80],
        input_path="$",
        result_path="$.result",
        output_path="$",
        resource="arn:aws:states:::sagemaker:createTransformJob.sync",
        parameters={
            "TransformJobName.$": "States.Format('{}-{}-{}', $.data_filter.TransformJobName , States.ArrayGetItem(States.StringSplit($$.Execution.StartTime, 'T'), 0) ,States.ArrayGetItem(States.StringSplit(States.UUID(),'-'), 0))",
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
            },
            "Tags": [
                {
                    "Key": "Exsell",
                    "Value": "ingestion"
                },
                {
                    "Key": "SagemakerCost",
                    "Value": "Transform"
                }
            ]
        },
        retry=retry
    )




  # {
  #   "Comment": "Insert your JSON here",
  #   "data_filter": {
  #     "context": "nbo.bu.share",
  #     "TrainingJobName": "nbo-bu-share-train-2023-03-40",
  # 	"TransformJobName": "nbo-bu-share-transform-2023-03-40",
  #     "AlgorithmName": "arn:aws:sagemaker:eu-west-1:985815980388:algorithm/maf-recommenderx-dask-rc7-781ad608939636b3a8c4ca212885bd19",
  #     "ModelPackageName": "nbo-bu-share-model-pacakge-2023-03-40",
  #     "ModelName": "nbo-bu-share-model-2023-03-40",
  #     "model_path": "s3://cvm-uat-conformed-d5b175d/features/nbo/model/nbo.bu.share/",
  #     "output_path": "s3://cvm-uat-conformed-d5b175d/features/nbo/output/nbo.bu.share/",
  #     "ModelDataUrl": "s3://cvm-uat-conformed-d5b175d/features/nbo/model/nbo.bu.share/nbo-bu-share-train-2023-03-40/output/model.tar.gz",
  #     "input_train_path": "s3://cvm-uat-conformed-d5b175d/features/nbo/input/train/nbo.bu.share/",
  # 	"input_transform_path": "s3://cvm-uat-conformed-d5b175d/features/nbo/input/transform/nbo.bu.share/",
  # 	"KmsKeyId": "e65fea64-7da6-4e27-bf4f-9cc2eb54ef0a"
  #   },
  #   "hyperparams": {
  #     "data_filter_context_index": "0",
  #     "values": {
  #       "epochs": "1",
  #       "embedding_dimensions": "32",
  #       "number_of_offers": "5",
  #       "learning_rate": "0.1"
  #     }
  #   }
  # }