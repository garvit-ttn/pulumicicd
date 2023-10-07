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


# NBO

Contexts = []
Contexts.append(Context("nbo.sharebrand", "sharebrand"))
Contexts.append(Context("nbo.sharemall", "sharemall"))
Contexts.append(Context("nbo.shareproposition3", "shareproposition3"))
# Contexts.append(Context("nbo.shareproposition1", "shareproposition1"))
Contexts.append(Context("nbo.shareproposition4", "shareproposition4"))
# Contexts.append(Context("nbo.shareproposition5", "shareproposition5"))

Contexts.append(Context("nbo.moe", "moe"))
Contexts.append(Context("nbo.ccmi", "ccmi"))
# # Contexts.append(Context("nbo.that", "that"))
# # Contexts.append(Context("nbo.cnb", "cnb"))
# Contexts.append(Context("nbo.lll", "lll"))
# Contexts.append(Context("nbo.lego", "lego"))
# Contexts.append(Context("nbo.als", "als"))
# Contexts.append(Context("nbo.cb2", "cb2"))
# Contexts.append(Context("nbo.anf", "anf"))
# Contexts.append(Context("nbo.hollister", "hollister"))
# Contexts.append(Context("nbo.shiseidoo", "shiseidoo"))
# Contexts.append(Context("nbo.pf", "pf"))
# Contexts.append(Context("nbo.lifestyle", "lifestyle"))
# Contexts.append(Context("nbo.sharepartners", "sharepartners"))


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
{hyperparameters}
}
"""

hyperparameters_brand_mall_03 = """
  "hyperparams": {
    "data_filter_context_index": "0",
    "values": {
      "training_period": "3",
      "testing_period": "3",
      "epochs": "30",
      "embedding_dimensions": "32",
      "train_batch_size": "8192",
      "test_batch_size": "8192",
      "item_batch_size": "512",
      "optimizer": "Adagrad",
      "learning_rate": "0.01",
      "early_stopping_metric": "loss",
      "early_stopping_min_delta": "20",
      "early_stopping_patience": "3",
      "early_stopping_mode": "min",
      "early_stopping_start_epoch": "10",
      "evaluate_train_data": "yes",
      "number_of_offers": "5",
      "num_leaves": "25"
    }
  }
"""

hyperparameters_04 = """
  "hyperparams": {
    "data_filter_context_index": "0",
    "values": {
      "training_period": "2",
      "testing_period": "2",
      "epochs": "30",
      "embedding_dimensions": "32",
      "train_batch_size": "8192",
      "test_batch_size": "8192",
      "item_batch_size": "512",
      "optimizer": "Adagrad",
      "learning_rate": "0.01",
      "early_stopping_metric": "loss",
      "early_stopping_min_delta": "20",
      "early_stopping_patience": "3",
      "early_stopping_mode": "min",
      "early_stopping_start_epoch": "10",
      "evaluate_train_data": "yes",
      "number_of_offers": "50",
      "num_leaves": "100"
    }
  }
"""


hyperparameters_05 = """
  "hyperparams": {
    "data_filter_context_index": "0",
    "values": {
      "training_period": "2",
      "testing_period": "2",
      "epochs": "30",
      "embedding_dimensions": "32",
      "train_batch_size": "8192",
      "test_batch_size": "8192",
      "item_batch_size": "512",
      "optimizer": "Adagrad",
      "learning_rate": "0.01",
      "early_stopping_metric": "loss",
      "early_stopping_min_delta": "20",
      "early_stopping_patience": "3",
      "early_stopping_mode": "min",
      "early_stopping_start_epoch": "10",
      "evaluate_train_data": "yes",
      "number_of_offers": "100",
      "num_leaves": "100"
    }
  }
"""


def get_right_hyperparameters(contex: str):
    response = hyperparameters_brand_mall_03
    match contex:
        case 'nbo.sharebrand' | 'nbo.sharemall' | 'nbo.shareproposition' | 'nbo.shareproposition03':
            response = hyperparameters_brand_mall_03
        case 'nbo.shareproposition04':
            response = hyperparameters_04
        case 'nbo.shareproposition05':
            response = hyperparameters_05
    return response


def get_step_function(context, short, lake_descriptor, kms_key_id, nbo_arn, train_flow_arn):
    inptujson = (input
                 .replace("{context}", context)
                 .replace("{model_path}", get_s3_path_for_feature(features.NboModel, lake_descriptor, context))
                 .replace("{input_train_path}",
                          get_s3_path_for_feature(features.NboTrainingInput, lake_descriptor, context))
                 .replace("{TrainingJobName}", str(context).replace('.', '-') + '-test4')
                 .replace("{KmsKeyId}", kms_key_id)
                 .replace("{AlgorithmName}", nbo_arn)
                 .replace("{hyperparameters}", get_right_hyperparameters(context))
                 )

    stepfunctionsstartrxecutionstep = StepFunctionsStartExecutionStep(
        state_id=f"Run-nbo-{short}"[0:80],
        integration_pattern=IntegrationPattern.WaitForCompletion,
        parameters={
            "StateMachineArn": train_flow_arn,
            "Name.$": f"States.Format('{{}}-{{}}', States.Format('nbo_{{}}_{{}}','{short}', States.ArrayGetItem(States.StringSplit($$.Execution.StartTime, 'T'), 0)) ,States.ArrayGetItem(States.StringSplit(States.UUID(),'-'), 0))",
            "Input": json.loads(inptujson)
        },
        input_path="$"
    )

    return stepfunctionsstartrxecutionstep


def generate_train_models_for_customer_profiles(
        workflow_execution_role_arn: str,
        train_flow_arn: str,
        lake_descriptor: str,
        kms_key_id: str,
        environment: str
) -> str:

    # PROD
    machine_max_prod = 1
    clv_arn_prod = "arn:aws:sagemaker:eu-west-1:985815980388:algorithm/maf-clv-dask-rc5-66dd3b54fd5738f2ab97d02b3123cc6f"
    rfm_arn_prod = "arn:aws:sagemaker:eu-west-1:985815980388:algorithm/maf-rfm-dask-rc13-46bfe3ea19ee3951a114956e2ea1ace5"
    nbo_arn_prod = "arn:aws:sagemaker:eu-west-1:985815980388:algorithm/maf-recommenderx-dask-rc20-0194f31b2f5b3d73b551ad2c03f57a17"

    # UAT
    machine_max_uat = 1
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

    train_parallel = Parallel(state_id=f"Run NBOs train step: {step}")

    for index, key in enumerate(Contexts):
        steps.append(get_step_function(key.context, key.short, lake_descriptor, kms_key_id, nbo_arn, train_flow_arn))

        # try:
        #     print(index)
        #     print(key.context)
        #     print(Contexts[index + 1].context)
        #     transform_catch = Catch(
        #         error_equals=["States.ALL"],
        #         next_step=get_step_function(Contexts[index + 1].context, Contexts[index + 1].short, lake_descriptor, kms_key_id, nbo_arn, train_flow_arn)
        #     )
        #     steps.append(get_step_function(key.context, key.short, lake_descriptor, kms_key_id, nbo_arn, train_flow_arn).add_catch(steps))
        #
        # except:
        #     print(key.context)
        #     steps.append(get_step_function(key.short, key.context, lake_descriptor, kms_key_id, nbo_arn, train_flow_arn))



        # inptujson = (input
        #              .replace("{context}", key.context)
        #              .replace("{model_path}", get_s3_path_for_feature(features.NboModel, lake_descriptor, key.context))
        #              .replace("{input_train_path}", get_s3_path_for_feature(features.NboTrainingInput, lake_descriptor, key.context))
        #              .replace("{TrainingJobName}", str(key.context).replace('.', '-')+'-test')
        #              .replace("{KmsKeyId}", kms_key_id)
        #              .replace("{AlgorithmName}", nbo_arn)
        #              .replace("{hyperparameters}", get_right_hyperparameters(key.context))
        #              )
        #
        # stepfunctionsstartrxecutionstep = StepFunctionsStartExecutionStep(
        #     state_id=f"Run-nbo-{key.short}"[0:80],
        #     integration_pattern=IntegrationPattern.WaitForCompletion,
        #     parameters={
        #         "StateMachineArn": train_flow_arn,
        #         "Name.$": f"States.Format('{{}}-{{}}', States.Format('nbo_{{}}_{{}}','{key.short}', States.ArrayGetItem(States.StringSplit($$.Execution.StartTime, 'T'), 0)) ,States.ArrayGetItem(States.StringSplit(States.UUID(),'-'), 0))",
        #         "Input": json.loads(inptujson)
        #     },
        #     input_path="$"
        # )



    flow = Workflow(
        name="cvm-nbo-train-workflow",
        role=workflow_execution_role_arn,
        definition=Chain(steps)
    )

    return flow.definition.to_json(pretty=True)