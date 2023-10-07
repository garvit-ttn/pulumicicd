import logging

import stepfunctions
from stepfunctions.steps import *
from stepfunctions.workflow import Workflow

from cvmdatalake.workflows import run_crawler

stepfunctions.set_stream_logger(level=logging.INFO)


def generate_share_audiences_workflow_content(
        workflow_execution_role_arn: str,
        share_audiences_landing_crawler: str, share_audiences_staging_crawler: str, share_audiences_conformed_crawler: str,
        ingestion_job: str, merging_job: str
) -> str:

    share_audiences_landing_crawler_step = run_crawler(share_audiences_landing_crawler)
    share_audiences_ingestion_step = GlueStartJobRunStep(
        state_id=f"Run-{ingestion_job}"[0:80],
        wait_for_completion=True,
        parameters={
            'JobName': ingestion_job
        }
    )

    share_audiences_staging_crawler_step = run_crawler(share_audiences_staging_crawler)
    share_audiences_merging_step = GlueStartJobRunStep(
        state_id=f"Run-{merging_job}"[0:80],
        wait_for_completion=True,
        parameters={
            'JobName': merging_job
        }
    )

    share_audiences_conformed_crawler_step = run_crawler(share_audiences_conformed_crawler)

    flow = Workflow(
        name="cvm-testy-workflow",
        role=workflow_execution_role_arn,
        definition=Chain([
            share_audiences_landing_crawler_step,
            share_audiences_ingestion_step,
            share_audiences_staging_crawler_step,
            share_audiences_merging_step,
            share_audiences_conformed_crawler_step,
            Succeed(state_id=f"Success-share_audiences-flow")
        ])
    )

    return flow.definition.to_json(pretty=True)
