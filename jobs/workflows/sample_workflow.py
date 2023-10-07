from stepfunctions.workflow import Workflow
from stepfunctions.steps import GlueStartJobRunStep, Chain


def generate_my_test_workflow_content(workflow_execution_role_arn: str, job_name_to_run: str) -> str:
    audience_builder_step = GlueStartJobRunStep(
        state_id=f"Run-{job_name_to_run}"[0:80],
        wait_for_completion=True,
        parameters={
            'JobName': job_name_to_run
        }
    )

    flow = Workflow(
        name="cvm-testy-workflow",
        role=workflow_execution_role_arn,
        definition=Chain([
            audience_builder_step,
        ])
    )

    return flow.definition.to_json(pretty=True)
