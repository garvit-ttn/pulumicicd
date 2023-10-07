import logging
import string
from functools import partial
from random import choice
from typing import Sequence

import stepfunctions
from pulumi import Input, Output, StackReference
from sagemaker.transformer import Transformer
from stepfunctions.steps import *
from stepfunctions.workflow import Workflow

from cvmdatalake import features, get_s3_path, transform_descriptor

stepfunctions.set_stream_logger(level=logging.INFO)

sm_jobs_config = {
    'nbo-inference': {
        'model_name': 'nbo-dask-scann-epoch1',
        'input_spec': features.NboTransformInput,
        'output_spec': features.NboOutput,
    },
    'clv-inference': {
        'model_name': 'clv-model-package-template',
        'input_spec': features.ClvTransformInput,
        'output_spec': features.ClvOutput,
    },
    'rfm-inference': {
        'model_name': 'clv-model-package-template',
        'input_spec': features.RfmTrainingInput,
        'output_spec': features.RfmOutput,
    }
}


def generate_workflow_definition_lift(
        sm_execution_role: Input[str],
        feature_for_inference_jobs: Sequence[Input[str]],
        customer_profile_enrichment_job: Input[str],
        data_loading_jobs: Sequence[Input[str]],
        lake_stack: StackReference
) -> Output[partial]:
    return Output.all(
        sm_execution_role=sm_execution_role,
        feature_for_inference_jobs=Output.all(*feature_for_inference_jobs).apply(lambda args: args),
        data_loading_jobs=Output.all(*data_loading_jobs).apply(lambda args: args),
        customer_profile_enrichment_job=customer_profile_enrichment_job,
        lake_descriptor=lake_stack.get_output('lake_descriptor').apply(transform_descriptor)
    ).apply(
        lambda args: partial(
            generate_workflow_definition,
            sm_execution_role=args['sm_execution_role'],
            feature_for_inference_jobs=args['feature_for_inference_jobs'],
            data_loading_jobs=args['data_loading_jobs'],
            customer_profile_enrichment_job=args['customer_profile_enrichment_job'],
            lake_descriptor=args['lake_descriptor']
        )
    )


def generate_workflow_definition(
        workflow_execution_role_arn: str, lake_descriptor: str,
        sm_execution_role: str,
        feature_for_inference_jobs: Sequence[str],
        data_loading_jobs: Sequence[str],
        customer_profile_enrichment_job: str
) -> str:
    run_feature_for_inference_jobs = Parallel(state_id="Run feature extraction for inference")
    for job_name in feature_for_inference_jobs:
        run_feature_for_inference_jobs.add_branch(
            GlueStartJobRunStep(
                state_id=f"Run-{job_name}"[0:80],
                wait_for_completion=True,
                parameters={
                    'JobName': job_name
                }
            )
        )

    sm_transform_jobs = ['nbo-inference', 'clv-inference']
    run_feature_sm_transformations = Parallel(state_id="Run CVM SageMaker algorithms")

    for job_name in sm_transform_jobs:
        job_cfg = sm_jobs_config[job_name]
        run_feature_sm_transformations.add_branch(
            TransformStep(
                state_id=f"Run-{job_name}"[0:80],
                wait_for_completion=True,
                job_name=job_name,
                model_name=job_cfg['model_name'],
                data=get_s3_path(job_cfg['input_spec'], lake_descriptor),
                content_type='text/csv',
                transformer=Transformer(
                    model_name=job_cfg['model_name'],
                    instance_type='ml.m5.24xlarge',
                    instance_count=1,
                    output_path=get_s3_path(job_cfg['output_spec'], lake_descriptor)
                )
            )
        )

    rfm_inference_steps = Chain()
    run_feature_sm_transformations.add_branch(rfm_inference_steps)

    rfm_inference_steps.append(Pass(state_id='Run-rfm-training'))
    rfm_inference_steps.append(training_step(lake_descriptor, sm_execution_role))

    enrichment_job = GlueStartJobRunStep(
        state_id=f"Run-{customer_profile_enrichment_job}"[0:80],
        wait_for_completion=True,
        parameters={
            'JobName': customer_profile_enrichment_job
        }
    )

    data_load = Chain()
    for job_name in data_loading_jobs:
        data_load.append(
            GlueStartJobRunStep(
                state_id=f"Run-{job_name}"[0:80],
                wait_for_completion=True,
                parameters={
                    'JobName': job_name
                }
            )
        )

    flow = Workflow(
        name="cvm-ingestion-workflow",
        role=workflow_execution_role_arn,
        definition=Chain([
            run_feature_for_inference_jobs,
            run_feature_sm_transformations,
            enrichment_job,
            data_load
        ])
    )

    return flow.definition.to_json(pretty=True)


def training_step(lake_descriptor, sm_execution_role):
    return Task(
        state_id='Run-rfm-inference',
        resource="arn:aws:states:::sagemaker:createTrainingJob.sync",
        parameters={
            "TrainingJobName": f"rfm-inference-(.$[String.UUID()])",
            "EnableManagedSpotTraining": True,
            "EnableNetworkIsolation": True,
            "RoleArn": sm_execution_role,
            "StoppingCondition": {
                "MaxRuntimeInSeconds": 86400,
                "MaxWaitTimeInSeconds": 86400
            },
            "AlgorithmSpecification": {
                "AlgorithmName": 'arn:aws:sagemaker:eu-west-1:985815980388:algorithm/maf-rfm-rc8-cd7eb8867c863bdc970c33ede762a810',
                "EnableSageMakerMetricsTimeSeries": True,
                "TrainingInputMode": "File"
            },
            "ResourceConfig": {
                "InstanceCount": 1,
                "InstanceType": "ml.m5.24xlarge",
                "VolumeSizeInGB": 200
            },
            "OutputDataConfig": {
                "KmsKeyId": "None",
                "S3OutputPath": get_s3_path(features.RfmOutput, lake_descriptor)
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
                            "S3Uri": get_s3_path(features.RfmTrainingInput, lake_descriptor),
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
