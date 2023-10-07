import logging

import stepfunctions
from stepfunctions.steps import *
from stepfunctions.workflow import Workflow

from cvmdatalake.workflows import run_crawler

stepfunctions.set_stream_logger(level=logging.INFO)


def generate_customer_profiles_workflow_content(
        workflow_execution_role_arn: str,
        customer_profiles_conformed_crawler: str, results_job: str,
        stats_job: str,
        c360_job: str
) -> str:
    customer_profiles_c360_job_step = GlueStartJobRunStep(
        state_id=f"Run-{c360_job}"[0:80],
        wait_for_completion=True,
        parameters={
            'JobName': c360_job

        }
    )
    customer_profiles_conformed_crawler_step = run_crawler(customer_profiles_conformed_crawler)
    customer_profiles_stats_step = GlueStartJobRunStep(
        state_id=f"Run-{stats_job}"[0:80],
        wait_for_completion=True,
        parameters={
            'JobName': stats_job

        }
    )
    customer_profiles_results_step = GlueStartJobRunStep(
        state_id=f"Run-{results_job}"[0:80],
        wait_for_completion=True,
        parameters={
            'JobName': results_job
        }
    )

    flow = Workflow(
        name="cvm-testy-workflow",
        role=workflow_execution_role_arn,
        definition=Chain([
            customer_profiles_c360_job_step,
            customer_profiles_conformed_crawler_step,
            customer_profiles_stats_step,
            customer_profiles_results_step,
        ])
    )

    return flow.definition.to_json(pretty=True)
