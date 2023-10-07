from functools import partial
from typing import Sequence, Tuple, Dict

from stepfunctions.steps import Chain, StepFunctionsStartExecutionStep, Parallel
from stepfunctions.workflow import Workflow

from pulumi import StackReference, Input, Output

from cvmdatalake.pulumi import transform_descriptor


def generate_workflow_definition_lift(
        transform_flows: dict[Input[str], Input[str]],
        lake_stack: StackReference
) -> Output[partial]:
    return Output.all(
        transform_flows=Output.all(**transform_flows),
        lake_descriptor=lake_stack.get_output('lake_descriptor').apply(transform_descriptor)
    ).apply(
        lambda args: partial(
            generate_workflow_definition,
            lake_descriptor=args['lake_descriptor'],
            transform_flows=args['transform_flows'],
        )
    )


def generate_workflow_definition(
        workflow_execution_role_arn: str, lake_descriptor: str,
        transform_flows: dict[str, str]
) -> str:
    run_all_contexts = Parallel(state_id="Run algorithms transform for all contexts")
    for context, flow in transform_flows.items():

        run_all_contexts.add_branch(
            StepFunctionsStartExecutionStep(
                state_id="",
                parameters={
                    "comment": "test"
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
