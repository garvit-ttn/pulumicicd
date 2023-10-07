from functools import partial

from pulumi import StackReference, Input, Output
from stepfunctions.steps import Chain, StepFunctionsStartExecutionStep, Parallel
from stepfunctions.workflow import Workflow

from cvmdatalake import DataFilterContext
from cvmdatalake.data_filters import hyper_parameters, Algorithm
from cvmdatalake.pulumi import transform_descriptor


def generate_workflow_definition_lift(
        train_flows: dict[Input[str], Input[str]],
        lake_stack: StackReference
) -> Output[partial]:
    return Output.all(
        train_flows=Output.all(**train_flows),
        lake_descriptor=lake_stack.get_output('lake_descriptor').apply(transform_descriptor)
    ).apply(
        lambda args: partial(
            generate_workflow_definition,
            lake_descriptor=args['lake_descriptor'],
            train_flows=args['train_flows'],
        )
    )


def generate_workflow_definition(
        workflow_execution_role_arn: str, lake_descriptor: str,
        train_flows: dict[str, str]
) -> str:
    run_all_contexts = Parallel(state_id="Run algorithms train for all contexts")

    for context in DataFilterContext:
        if context == DataFilterContext.default:
            continue

        algorithm = [a for a in Algorithm if a in context][0]

        run_all_contexts.add_branch(
            StepFunctionsStartExecutionStep(
                state_id=f"train-{context}",
                parameters={
                    "StateMachineArn": train_flows[algorithm],
                    "Input": {
                        "data_filter": {
                            "context": context,
                            "model_path": f"s3://cvm-uat-conformed-d5b175d/features/{algorithm}/model/{context}/",
                            "output_path": f"s3://cvm-uat-conformed-d5b175d/features/{algorithm}/output/",
                            "input_path": f"s3://cvm-uat-conformed-d5b175d/features/{algorithm}/input/train/{context}/"
                        },
                        "hyperparams": (
                            hp if (hp := hyper_parameters.get(context)) is None
                            else hyper_parameters[f'default.{algorithm}']
                        )
                    }
                }
            )
        )

    flow = Workflow(
        name="cvm-transform-all-workflow",
        role=workflow_execution_role_arn,
        definition=Chain([
            run_all_contexts
        ])
    )

    return flow.definition.to_json(pretty=True)
