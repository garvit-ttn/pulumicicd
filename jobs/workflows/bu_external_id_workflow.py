import logging

import stepfunctions
from stepfunctions.steps import *
from stepfunctions.workflow import Workflow

from cvmdatalake.workflows import run_crawler

stepfunctions.set_stream_logger(level=logging.INFO)


def generate_bu_external_id_workflow_content(
        workflow_execution_role_arn: str,
        bu_external_id_landing_crawler: str, bu_external_id_staging_crawler: str, bu_external_id_conformed_crawler: str,
        ingestion_job: str, merging_job: str
) -> str:

    bu_external_id_landing_crawler_step = run_crawler(bu_external_id_landing_crawler)
    bu_external_id_ingestion_step = GlueStartJobRunStep(
        state_id=f"Run-{ingestion_job}"[0:80],
        wait_for_completion=True,
        parameters={
            'JobName': ingestion_job
        }
    )

    bu_external_id_staging_crawler_step = run_crawler(bu_external_id_staging_crawler)
    bu_external_id_merging_step = GlueStartJobRunStep(
        state_id=f"Run-{merging_job}"[0:80],
        wait_for_completion=True,
        parameters={
            'JobName': merging_job
        }
    )

    bu_external_id_conformed_crawler_step = run_crawler(bu_external_id_conformed_crawler)

    flow = Workflow(
        name="cvm-testy-workflow",
        role=workflow_execution_role_arn,
        definition=Chain([
            bu_external_id_landing_crawler_step,
            bu_external_id_ingestion_step,
            bu_external_id_staging_crawler_step,
            bu_external_id_merging_step,
            bu_external_id_conformed_crawler_step,
            Succeed(state_id=f"Success-bu-external-id-flow")
        ])
    )

    return flow.definition.to_json(pretty=True)
