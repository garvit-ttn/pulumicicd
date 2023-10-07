

import logging
from typing import Sequence, Optional

import stepfunctions
from stepfunctions.steps import *
from stepfunctions.steps.states import State
from stepfunctions.workflow import Workflow

stepfunctions.set_stream_logger(level=logging.INFO)

def generate_refresh_profiles_workflow_content(
        workflow_execution_role_arn: str,
         refresh_job: str,

) -> str:

    refresh_profiles_job_step = GlueStartJobRunStep(
        state_id=f"Run-{refresh_job}"[0:80],
        wait_for_completion=True,
        parameters={
            'JobName': refresh_job

        }
    )

    flow = Workflow(
        name="cvm-testy-workflow",
        role=workflow_execution_role_arn,
        definition=Chain([
            refresh_profiles_job_step,
        ])
    )

    return flow.definition.to_json(pretty=True)
