import logging
from typing import Sequence, Optional
import json
import stepfunctions
from stepfunctions.steps import *
from stepfunctions.steps.integration_resources import IntegrationPattern
from stepfunctions.steps.states import State
from stepfunctions.workflow import Workflow
from cvmdatalake import landing, staging, conformed, features, get_s3_path_for_feature

stepfunctions.set_stream_logger(level=logging.INFO)

class Context:
    def __init__(self, context, short):
        self.context = context
        self.short = short


# CLV
Contexts = []

# RFM

Contexts.append(Context("rfm.shareproposition1", "shareproposition1"))
Contexts.append(Context("rfm.carrefour", "carrefour"))
Contexts.append(Context("rfm.als", "als"))
Contexts.append(Context("rfm.sharebrand", "sharebrand"))
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


input = """
{
  "Comment": "Running Train for {context}",
  "data_filter": {
    "context": "{context}",
    "TrainingJobName": "{TrainingJobName}",   
    "AlgorithmName": "{AlgorithmName}",
    "ModelPackageName": "{TrainingJobName}-model-pacakge",
    "ModelName": "{TrainingJobName}-model",
    "model_path": "{model_path}/",   
    "ModelDataUrl": "{model_path}/",
    "input_train_path": "{input_train_path}/",   
    "KmsKeyId": "{KmsKeyId}"
  },
  "hyperparams": {
    "data_filter_context_index": "0",
    "values": {
        "column_1": "IDI_COUNTERPARTY",
        "column_2": ""
    }
  }
}
"""

def generate_train_models_for_customer_profiles(
        workflow_execution_role_arn: str,
        train_flow_arn: str,
        lake_descriptor: str,
        kms_key_id: str,
        environment: str
) -> str:

    # PROD
    machine_max_prod = 15
    clv_arn_prod = "arn:aws:sagemaker:eu-west-1:985815980388:algorithm/maf-clv-dask-rc5-66dd3b54fd5738f2ab97d02b3123cc6f"
    rfm_arn_prod = "arn:aws:sagemaker:eu-west-1:985815980388:algorithm/maf-rfm-dask-rc13-46bfe3ea19ee3951a114956e2ea1ace5"
    nbo_arn_prod = "arn:aws:sagemaker:eu-west-1:985815980388:algorithm/maf-recommenderx-dask-rc20-0194f31b2f5b3d73b551ad2c03f57a17"

    # UAT
    machine_max_uat = 2
    clv_arn_uat = "arn:aws:sagemaker:eu-west-1:985815980388:algorithm/maf-clv-dask-rc5-66dd3b54fd5738f2ab97d02b3123cc6f"
    rfm_arn_uat = ""
    nbo_arn_uat = ""

    steps = []
    sanynam_count = 1
    step = 1

    if environment == 'prod':
        machine_max = machine_max_prod
        clv_arn = clv_arn_prod
        rfm_arn = rfm_arn_prod
        nbo_arn = nbo_arn_prod
    else:
        machine_max = machine_max_uat
        clv_arn = clv_arn_uat
        rfm_arn = rfm_arn_uat
        nbo_arn = nbo_arn_uat


    train_parallel = Parallel(state_id=f"Run RFMs train step: {step}")
    for key in Contexts:
        inptujson = (input
                     .replace("{context}", key.context)
                     .replace("{model_path}", get_s3_path_for_feature(features.RfmModel, lake_descriptor, key.context))
                     .replace("{input_train_path}", get_s3_path_for_feature(features.RfmTrainingInput, lake_descriptor, key.context))
                     .replace("{TrainingJobName}", str(key.context).replace('.', '-'))
                     .replace("{KmsKeyId}", kms_key_id)
                     .replace("{AlgorithmName}", rfm_arn)
                     )

        stepfunctionsstartrxecutionstep = StepFunctionsStartExecutionStep(
            state_id=f"Run-rfm-{key.short}"[0:80],
            integration_pattern=IntegrationPattern.WaitForCompletion,
            parameters={
                "StateMachineArn": train_flow_arn,
                "Name.$": f"States.Format('{{}}-{{}}', States.Format('rfm_{{}}_{{}}','{key.short}', States.ArrayGetItem(States.StringSplit($$.Execution.StartTime, 'T'), 0)) ,States.ArrayGetItem(States.StringSplit(States.UUID(),'-'), 0))",
                "Input": json.loads(inptujson)
            },
            input_path="$"
        )

        train_parallel.add_branch(stepfunctionsstartrxecutionstep)
        if sanynam_count < machine_max:
            sanynam_count += 1
        else:
            steps.append(train_parallel)
            step += 1
            sanynam_count = 1
            train_parallel = Parallel(state_id=f"Run RFM3s train step: {step}")

    if step * machine_max - sanynam_count <= len(Contexts) or step == 1:
        steps.append(train_parallel)

    flow = Workflow(
        name="cvm-rfm-train-workflow",
        role=workflow_execution_role_arn,
        definition=Chain(steps)
    )

    return flow.definition.to_json(pretty=True)