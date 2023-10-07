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


# # CLV
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

Contexts.append(Context("clv.moeluxury", "moeluxury"))
Contexts.append(Context("clv.moegroceries", "moegroceries"))
Contexts.append(Context("clv.moefb", "moefb"))
Contexts.append(Context("clv.moefashion", "moefashion"))
Contexts.append(Context("clv.ccmifashio", "ccmifashio"))
Contexts.append(Context("clv.ccmifurnitre", "ccmifurnitre"))
Contexts.append(Context("clv.ccmifb", "ccmifb"))
Contexts.append(Context("clv.shareluxury", "shareluxury"))
Contexts.append(Context("clv.ccmiluxury", "ccmiluxury"))
Contexts.append(Context("clv.ccmibeauty", "ccmibeauty"))


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
      "penalizer_coef": "0.01",
      "months_to_predict": "6",
      "testing_period": "12",
      "max_correlation_coef": "0.7",
      "minimum_purchases": "1",
      "training_period": "12",
      "discount_rate": "0.01"
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


    train_parallel = Parallel(state_id=f"Run CLVs train step: {step}")
    for key in Contexts:
        inptujson = (input
                     .replace("{context}", key.context)
                     .replace("{model_path}", get_s3_path_for_feature(features.ClvModel, lake_descriptor, key.context))
                     .replace("{input_train_path}", get_s3_path_for_feature(features.ClvTrainingInput, lake_descriptor, key.context))
                     .replace("{TrainingJobName}", str(key.context).replace('.', '-'))
                     .replace("{KmsKeyId}", kms_key_id)
                     .replace("{AlgorithmName}", clv_arn)
                     )

        stepfunctionsstartrxecutionstep = StepFunctionsStartExecutionStep(
            state_id=f"Run-clv-{key.short}"[0:80],
            integration_pattern=IntegrationPattern.WaitForCompletion,
            parameters={
                "StateMachineArn": train_flow_arn,
                "Name.$": f"States.Format('{{}}-{{}}', States.Format('clv_{{}}_{{}}','{key.short}', States.ArrayGetItem(States.StringSplit($$.Execution.StartTime, 'T'), 0)) ,States.ArrayGetItem(States.StringSplit(States.UUID(),'-'), 0))",
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
            train_parallel = Parallel(state_id=f"Run CLVs train step: {step}")

    if step * machine_max - sanynam_count <= len(Contexts) or step == 1:
        steps.append(train_parallel)

    flow = Workflow(
        name="cvm-clv-train-workflow",
        role=workflow_execution_role_arn,
        definition=Chain(steps)
    )

    return flow.definition.to_json(pretty=True)