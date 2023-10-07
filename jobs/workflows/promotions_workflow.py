import logging

import stepfunctions
from stepfunctions.steps import *
from stepfunctions.workflow import Workflow

from cvmdatalake.workflows import run_crawler

stepfunctions.set_stream_logger(level=logging.INFO)


def generate_promotions_workflow_content(
        workflow_execution_role_arn: str,
        promotions_landing_crawler: str, promotions_staging_crawler: str, promotions_conformed_crawler: str,
        ingestion_job: str, merging_job: str, offerbank_job: str
) -> str:
    promotions_landing_crawler_step = run_crawler(promotions_landing_crawler)
    promotion_ingestion_step = GlueStartJobRunStep(
        state_id=f"Run-{ingestion_job}"[0:80],
        wait_for_completion=True,
        parameters={
            'JobName': ingestion_job

        }
    )

    promotions_staging_crawler_step = run_crawler(promotions_staging_crawler)
    promotion_merging_step = GlueStartJobRunStep(
        state_id=f"Run-{merging_job}"[0:80],
        wait_for_completion=True,
        parameters={
            'JobName': merging_job
        }
    )

    promotions_conformed_crawler_step = run_crawler(promotions_conformed_crawler)
    load_offer_bank_job_step = GlueStartJobRunStep(
        state_id=f"Run-{offerbank_job}"[0:80],
        wait_for_completion=True,
        parameters={
            'JobName': offerbank_job
        }
    )

    flow = Workflow(
        name="cvm-testy-workflow",
        role=workflow_execution_role_arn,
        definition=Chain([
            promotions_landing_crawler_step,
            promotion_ingestion_step,
            promotions_staging_crawler_step,
            promotion_merging_step,
            promotions_conformed_crawler_step,
            load_offer_bank_job_step,
        ])
    )

    return flow.definition.to_json(pretty=True)
