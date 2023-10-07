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
from pulumi import Input, StackReference, Output

stepfunctions.set_stream_logger(level=logging.INFO)

def generate_run_all(
        workflow_execution_role_arn: str,
        ingestion_flow_arn: str,
        clv_train_flow_arn: str,
        rfm_train_flow_arn: str,
        calculated_kpi_job_name: str,
        prepare_data_for_customer_profiles_flow_arn: str,
        build_customer_profiles_workflow_flow_arn: str,
        lake_descriptor: str,
        environment: str
) -> str:

    build_and_send_step_6 = StepFunctionsStartExecutionStep(
            state_id=f"Run-build-and-send-customer-profiles-workflow"[0:80],
            integration_pattern=IntegrationPattern.WaitForCompletion,
            parameters={
                "StateMachineArn": build_customer_profiles_workflow_flow_arn,
                "Name.$": f"States.Format('{{}}-{{}}', States.Format('{{}}_{{}}','build-and-send-customer-profiles', States.ArrayGetItem(States.StringSplit($$.Execution.StartTime, 'T'), 0)) ,States.ArrayGetItem(States.StringSplit(States.UUID(),'-'), 0))",
                "Input.$": "$"
            },
            input_path="$"
    )

    calculations_kpi_step_5_catch = Catch(
        error_equals=["States.ALL"],
        next_step=build_and_send_step_6
    )

    calculations_kpi_step_5 = GlueStartJobRunStep(
            state_id=f"Run-calculations-kpi-job"[0:80],
            wait_for_completion=True,
            parameters={
                'JobName': calculated_kpi_job_name
            },
            catch=calculations_kpi_step_5_catch
    )

    prepare_data_step_step_4_catch = Catch(
        error_equals=["States.ALL"],
        next_step=calculations_kpi_step_5
    )

    prepare_data_step_4 = StepFunctionsStartExecutionStep(
        state_id=f"Run-prepare-data-for-customer-profiles-workflow"[0:80],
        integration_pattern=IntegrationPattern.WaitForCompletion,
        parameters={
            "StateMachineArn": prepare_data_for_customer_profiles_flow_arn,
            "Name.$": f"States.Format('{{}}-{{}}', States.Format('{{}}_{{}}','prepare-data-for-customer-profiles', States.ArrayGetItem(States.StringSplit($$.Execution.StartTime, 'T'), 0)) ,States.ArrayGetItem(States.StringSplit(States.UUID(),'-'), 0))",
            "Input.$": "$"
        },
        input_path="$",
        catch=prepare_data_step_step_4_catch
    )

    rfm_train_step_3_catch = Catch(
        error_equals=["States.ALL"],
        next_step=prepare_data_step_4
    )

    rfm_train_step_3 = StepFunctionsStartExecutionStep(
        state_id=f"Run-rfm-train-workflow"[0:80],
        integration_pattern=IntegrationPattern.WaitForCompletion,
        parameters={
            "StateMachineArn": rfm_train_flow_arn,
            "Name.$": f"States.Format('{{}}-{{}}', States.Format('{{}}_{{}}','rfm-train', States.ArrayGetItem(States.StringSplit($$.Execution.StartTime, 'T'), 0)) ,States.ArrayGetItem(States.StringSplit(States.UUID(),'-'), 0))",
            "Input.$": "$"
        },
        input_path="$",
        catch=rfm_train_step_3_catch
    )

    clv_train_step_2_catch = Catch(
        error_equals=["States.ALL"],
        next_step=rfm_train_step_3
    )

    clv_train_step_2 = StepFunctionsStartExecutionStep(
        state_id=f"Run-clv-train-workflow"[0:80],
        integration_pattern=IntegrationPattern.WaitForCompletion,
        parameters={
            "StateMachineArn": clv_train_flow_arn,
            "Name.$": f"States.Format('{{}}-{{}}', States.Format('{{}}_{{}}','clv-train', States.ArrayGetItem(States.StringSplit($$.Execution.StartTime, 'T'), 0)) ,States.ArrayGetItem(States.StringSplit(States.UUID(),'-'), 0))",
            "Input.$": "$"
        },
        input_path="$",
        catch=clv_train_step_2_catch
    )

    ingestion_step_1_catch = Catch(
        error_equals=["States.ALL"],
        next_step=clv_train_step_2
    )

    ingestion_step_1 = StepFunctionsStartExecutionStep(
        state_id=f"Run-ingestion-workflow"[0:80],
        integration_pattern=IntegrationPattern.WaitForCompletion,
        parameters={
            "StateMachineArn": ingestion_flow_arn,
            "Name.$": f"States.Format('{{}}-{{}}', States.Format('{{}}_{{}}','ingestion-', States.ArrayGetItem(States.StringSplit($$.Execution.StartTime, 'T'), 0)) ,States.ArrayGetItem(States.StringSplit(States.UUID(),'-'), 0))",
            "Input.$": "$"
        },
        input_path="$",
        catch=ingestion_step_1_catch
    )
    steps = []

    steps.append(ingestion_step_1)
    steps.append(clv_train_step_2)
    steps.append(rfm_train_step_3)
    steps.append(prepare_data_step_4)
    steps.append(calculations_kpi_step_5)
    steps.append(build_and_send_step_6)

    flow = Workflow(
        name="cvm-run-all-workflow",
        role=workflow_execution_role_arn,
        definition=Chain(steps)
    )

    return flow.definition.to_json(pretty=True)
