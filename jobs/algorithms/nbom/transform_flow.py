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

    nbom_job_name = "nbom-inference"

    nbom_inference_job = transform_step(nbom_job_name)

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
        state_id=f"Start-nbom-crawler_name",
        resource="arn:aws:states:::aws-sdk:glue:startCrawler",
        parameters={
            'Name': find_in_sequence(conformed_crawlers, "profile_algorithms_output")
        }
    )

    flow = Workflow(
        name="cvm-nbom-transform-workflow",
        role=workflow_execution_role_arn,
        definition=Chain([
            feature_prep_job,
            # nbom_inference_job,
            # output_to_predictions_job,
            # start_crawler
        ])
    )

    return flow.definition.to_json(pretty=True)


def transform_step(nbom_job_name):
    return Task(
        state_id=f"Run-{nbom_job_name}"[0:80],
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
  #     "context": "nbom.bu.share",
  #     "TrainingJobName": "nbom-bu-share-train-2023-03-40",
  # 	"TransformJobName": "nbom-bu-share-transform-2023-03-40",
  #     "AlgorithmName": "arn:aws:sagemaker:eu-west-1:985815980388:algorithm/maf-recommenderx-dask-rc7-781ad608939636b3a8c4ca212885bd19",
  #     "ModelPackageName": "nbom-bu-share-model-pacakge-2023-03-40",
  #     "ModelName": "nbom-bu-share-model-2023-03-40",
  #     "model_path": "s3://cvm-uat-conformed-d5b175d/features/nbom/model/nbom.bu.share/",
  #     "output_path": "s3://cvm-uat-conformed-d5b175d/features/nbom/output/nbom.bu.share/",
  #     "ModelDataUrl": "s3://cvm-uat-conformed-d5b175d/features/nbom/model/nbom.bu.share/nbom-bu-share-train-2023-03-40/output/model.tar.gz",
  #     "input_train_path": "s3://cvm-uat-conformed-d5b175d/features/nbom/input/train/nbom.bu.share/",
  # 	"input_transform_path": "s3://cvm-uat-conformed-d5b175d/features/nbom/input/transform/nbom.bu.share/",
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