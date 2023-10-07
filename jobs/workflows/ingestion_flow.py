import logging
from typing import Sequence, Optional

import stepfunctions
from stepfunctions.steps import *
from stepfunctions.steps.states import State
from stepfunctions.workflow import Workflow

stepfunctions.set_stream_logger(level=logging.INFO)


def run_crawler_custom(
        crawler_name: str,
        next_step: Optional[State | Chain] = None,
        wait_for_results_seconds: int = 30,
        invocation_identifier: str = 'a',
) -> Chain:
    retry = Retry(
        error_equals=["States.ALL"],
        interval_seconds=5,
        max_attempts=5,
        backoff_rate=2,
    )

    get_crawler_status = Task(
        state_id=f"GetStatus-{crawler_name}-{invocation_identifier}",
        resource="arn:aws:states:::aws-sdk:glue:getCrawler",
        retry=retry,
        parameters={
            'Name': crawler_name
        }
    )

    wait_for_crawler = Wait(
        state_id=f"Wait-{crawler_name}-{invocation_identifier}",
        seconds=wait_for_results_seconds
    )

    crawlers_catch = Catch(
        error_equals=["States.ALL"],
        next_step=get_crawler_status
    )

    start_crawler = Task(
        state_id=f"Start-{crawler_name}-{invocation_identifier}",
        resource="arn:aws:states:::aws-sdk:glue:startCrawler",
        catch=crawlers_catch,
        parameters={
            'Name': crawler_name
        },
        retry=Retry(
            error_equals=["States.ALL"],
            interval_seconds=5,
            max_attempts=2,
            backoff_rate=2,
        )
    )

    if next_step is None:
        next_step = Succeed(state_id=f"Success-{crawler_name}-{invocation_identifier}")

    check_crawler_status = Choice(state_id=f"CheckStatus-{crawler_name}-{invocation_identifier}")
    check_crawler_status.default_choice(next_step)

    check_crawler_status.add_choice(
        rule=ChoiceRule.StringEquals(variable='$.Crawler.State', value='RUNNING'),
        next_step=wait_for_crawler
    )

    return Chain([start_crawler, wait_for_crawler, get_crawler_status, check_crawler_status])


def ingestion_workflow_definition(
        workflow_execution_role_arn: str,
        prep_jobs: Sequence[str],
        landing_to_staging_jobs: Sequence[str],
        staging_to_conformed_jobs: Sequence[str],
        landing_crawlers: Sequence[str],
        staging_crawlers: Sequence[str],
        conformed_crawlers: Sequence[str],
        post_ingestion_jobs: Sequence[str]
) -> str:
    steps = []

    run_landing_crawlers_step = Parallel(state_id="Run Landing crawlers")
    for crl_name in landing_crawlers:
        run_landing_crawlers_step.add_branch(run_crawler_custom(crl_name))

    run_landing_to_staging_step = Parallel(state_id="Run Landing to Staging Jobs")
    for job_name in landing_to_staging_jobs:
        run_landing_to_staging_step.add_branch(
            GlueStartJobRunStep(
                state_id=f"Run-{job_name}"[0:80],
                wait_for_completion=True,
                parameters={
                    'JobName': job_name
                }
            ))

    run_staging_to_conformed_step = Parallel(state_id="Run Staging to Conformed Jobs")
    for job_name in staging_to_conformed_jobs:
        run_staging_to_conformed_step.add_branch(
            GlueStartJobRunStep(
                state_id=f"Run-{job_name}"[0:80],
                wait_for_completion=True,
                parameters={
                    'JobName': job_name
                }
            ))

    run_conformed_crawlers_step = Parallel(state_id="Run Conformed crawlers")
    for crl_name in conformed_crawlers:
        run_conformed_crawlers_step.add_branch(run_crawler_custom(crl_name))

    run_staging_crawlers_step = Parallel(state_id="Run Staging crawlers")
    for crl_name in staging_crawlers:
        run_staging_crawlers_step.add_branch(run_crawler_custom(crl_name))

    parallelize_staging_crawlers = Parallel(state_id="Parallelize Staging crawlers")
    parallelize_staging_crawlers.add_branch(run_staging_crawlers_step)
    parallelize_staging_crawlers.add_branch(Chain([
        run_staging_to_conformed_step,
        run_conformed_crawlers_step,
        # run_post_ingestion_step
    ]))

    steps.append(run_landing_crawlers_step)
    steps.append(run_landing_to_staging_step)
    steps.append(parallelize_staging_crawlers)

    flow = Workflow(
        name="cvm-ingestion-workflow",
        role=workflow_execution_role_arn,
        definition=Chain(steps),
    )

    return flow.definition.to_json(pretty=True)
