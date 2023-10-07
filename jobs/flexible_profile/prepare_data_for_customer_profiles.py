import logging
from typing import Sequence, Optional


import json
import stepfunctions
from stepfunctions.steps import *
from stepfunctions.steps.integration_resources import IntegrationPattern
from stepfunctions.steps.states import State
from stepfunctions.workflow import Workflow
from cvmdatalake import landing, staging, conformed, features, get_s3_path_for_feature
from cvmdatalake.data_filters import DataFilterContext
import random

stepfunctions.set_stream_logger(level=logging.INFO)


class Context:
    def __init__(self, context, short):
        self.context = context
        self.short = short


## CLV
Contexts = []
Contexts.append(Context("clv.carrefour", "carrefour"))
Contexts.append(Context("clv.share", "share"))
Contexts.append(Context("clv.skidubai", "skidubai"))
Contexts.append(Context("clv.magicplanet", "magicplanet"))
Contexts.append(Context("clv.voxcinemas", "voxcinemas"))
Contexts.append(Context("clv.ofemirates", "ofemirates"))
Contexts.append(Context("clv.lululemon", "lululemon"))
Contexts.append(Context("clv.citycentredeira", "citycentredeira"))
Contexts.append(Context("clv.citycentremirdif", "citycentremirdif"))
Contexts.append(Context("clv.allsaints", "allsaints"))
Contexts.append(Context("clv.cratebarrel", "cratebarrel"))
Contexts.append(Context("clv.lec", "lec"))
Contexts.append(Context("clv.smbu", "smbu"))
Contexts.append(Context("clv.lifestyle", "lifestyle"))
#
# # RFM
#
Contexts.append(Context("rfm.sharebrand", "sharebrand"))
Contexts.append(Context("rfm.shareproposition1", "shareproposition1"))
Contexts.append(Context("rfm.carrefour", "carrefour"))
Contexts.append(Context("rfm.als", "als"))
Contexts.append(Context("rfm.cnb", "cnb"))
Contexts.append(Context("rfm.lec", "lec"))
Contexts.append(Context("rfm.lifestyle", "lifestyle"))
Contexts.append(Context("rfm.lll", "lll"))
Contexts.append(Context("rfm.magicplanet", "magicplanet"))
Contexts.append(Context("rfm.skidubai", "skidubai"))
Contexts.append(Context("rfm.smbu", "smbu"))
Contexts.append(Context("rfm.vox", "vox"))
Contexts.append(Context("rfm.ccd", "ccd"))
Contexts.append(Context("rfm.ccmi", "ccmi"))
Contexts.append(Context("rfm.moe", "moe"))

# NBO

Contexts.append(Context("nbo.shareproposition3", "shareproposition3"))
Contexts.append(Context("nbo.shareproposition4", "shareproposition1"))
Contexts.append(Context("nbo.sharemall", "sharemall"))
Contexts.append(Context("nbo.sharebrand", "sharebrand"))
Contexts.append(Context("nbo.moe", "moe"))
Contexts.append(Context("nbo.ccmi", "ccmi"))

Contexts.append(Context("clv.moegroceries", "moegroceries"))
Contexts.append(Context("clv.moefb", "moefb"))
Contexts.append(Context("clv.moefashion", "moefashion"))
Contexts.append(Context("clv.ccmiluxury", "ccmiluxury"))
Contexts.append(Context("clv.ccmifashio", "ccmifashio"))
Contexts.append(Context("clv.ccmifurnitre", "ccmifurnitre"))
Contexts.append(Context("clv.ccmifb", "ccmifb"))
Contexts.append(Context("clv.shareluxury", "shareluxury"))
Contexts.append(Context("clv.ccmibeauty", "ccmibeauty"))
Contexts.append(Context("clv.moeluxury", "moeluxury"))

main_input = """
{
  "Comment": "Running Transform for {context}",
  "data_filter": {
    "context": "{context}",   
	"TransformJobName": "{TransformJobName}",      
    "ModelName": "{model_name}",  
    "output_path": "{output_path}/",      
	"input_transform_path": "{input_transform_path}/",
	"KmsKeyId": "{KmsKeyId}"
  }
}
"""


def get_right_path(input_or_ouptut: str, contex: str, lake_descriptor):
    algorithm = contex.split(".")[0]
    response = ''
    match algorithm:
        case 'clv':
            if input_or_ouptut == 'input':
                response = get_s3_path_for_feature(features.ClvTransformInput, lake_descriptor, contex)
            else:
                response = get_s3_path_for_feature(features.ClvOutput, lake_descriptor, contex)
        case 'rfm':
            if input_or_ouptut == 'input':
                response = get_s3_path_for_feature(features.RfmTransformInput, lake_descriptor, contex)
            else:
                response = get_s3_path_for_feature(features.RfmOutput, lake_descriptor, contex)
        case 'nbo':
            if input_or_ouptut == 'input':
                response = get_s3_path_for_feature(features.NboTransformInput, lake_descriptor, contex)
            else:
                response = get_s3_path_for_feature(features.NboOutput, lake_descriptor, contex)
    return response


def get_right_arn(contex: str, clv_transform_flow_arn, rfm_transform_flow_arn, nbo_transform_flow_arn):
    algorithm = contex.split(".")[0]
    response = ''
    match algorithm:
        case 'clv':
            response = clv_transform_flow_arn
        case 'rfm':
            response = rfm_transform_flow_arn
        case 'nbo':
            response = nbo_transform_flow_arn
    return response



def generate_prepare_data_for_customer_profiles(
        workflow_execution_role_arn: str,
        clv_transform_flow_arn: str,
        rfm_transform_flow_arn: str,
        nbo_transform_flow_arn: str,
        lake_descriptor: str,
        kms_key_id: str,
        lambda_sagemaker_get_latest_models: str,
        environment: str
) -> str:
    steps = []

    machine_max_uat = 2
    machine_max_prod = 15
    sanynam_count = 1
    step = 1

    if environment == 'prod':
        machine_max = machine_max_prod
    else:
        machine_max = machine_max_uat

    transform_parallel = Parallel(state_id=f"Run transform step: {step}")

    for key in Contexts:
        get_lambda_model_and_run_transform_pair = []

        inptujson = (main_input
                     .replace("{context}", key.context)
                     .replace("{TransformJobName}", str(key.context).replace('.', '-'))
                     .replace("{output_path}", get_right_path('output', key.context, lake_descriptor))
                     .replace("{input_transform_path}", get_right_path('input', key.context, lake_descriptor))
                     .replace("{KmsKeyId}", kms_key_id)
                     )

        get_lambda_model_and_run_transform_pair.append(
            LambdaStep(
                state_id=f"Get-model-{str(key.context).replace('.','-')}"[0:80],
                wait_for_callback=False,
                comment="Comments to do",
                parameters={
                    "FunctionName": lambda_sagemaker_get_latest_models,
                    "Payload": {
                        "input": json.loads(inptujson)
                    }
                },
                result_path='$',
                retry=Retry(
                    error_equals=["States.ALL"],
                    interval_seconds=random.randint(2, 40),
                    max_attempts=6,
                    backoff_rate=random.randint(2, 10),
                )
            )
        )

        get_lambda_model_and_run_transform_pair.append(
            StepFunctionsStartExecutionStep(
                state_id=f"Run-transform-{str(key.context).replace('.','-')}"[0:80],
                integration_pattern=IntegrationPattern.WaitForCompletion,
                parameters={
                    "StateMachineArn": get_right_arn(key.context, clv_transform_flow_arn, rfm_transform_flow_arn, nbo_transform_flow_arn),
                    "Name.$": f"States.Format('{{}}-{{}}', States.Format('{{}}_{{}}','{str(key.context).replace('.','_')}', States.ArrayGetItem(States.StringSplit($$.Execution.StartTime, 'T'), 0)) ,States.ArrayGetItem(States.StringSplit(States.UUID(),'-'), 0))",
                    "Input.$": "$.Payload.input"
                },
                input_path="$"
            )
        )

        get_lambda_model_and_run_transform_pair.append(Pass(state_id=f"Pass-{key.context.replace('.','-')}"))

        transform_parallel.add_branch(Chain(get_lambda_model_and_run_transform_pair))
        if sanynam_count < machine_max:
            sanynam_count += 1
        else:
            pass_transform = Pass(state_id=f"Pass-error-{step}")
            transform_catch = Catch(
                error_equals=["States.ALL"],
                next_step=pass_transform
            )
            transform_parallel.add_catch(transform_catch)
            steps.append(transform_parallel)
            step += 1
            sanynam_count = 0
            transform_parallel = Parallel(state_id=f"Run transform step: {step}")


    if step * machine_max - sanynam_count <= len(Contexts) or step == 1:
        steps.append(transform_parallel)

    flow = Workflow(
        name="cvm-transform-workflow",
        role=workflow_execution_role_arn,
        definition=Chain(steps)
    )

    return flow.definition.to_json(pretty=True)
