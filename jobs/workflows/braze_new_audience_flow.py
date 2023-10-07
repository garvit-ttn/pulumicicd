from stepfunctions.steps import GlueStartJobRunStep, Chain, Succeed
from stepfunctions.workflow import Workflow

from cvmdatalake.workflows import run_crawler


def generate_workflow_definition(
        workflow_execution_role_arn: str,
        audience_builder_job: str, braze_push_job: str,
        audience_payload_crawler: str,
        audience_array_crawler: str, audience_crawler: str, audience_braze_crawler: str,
        delta_mapping_crawler: str, offer_braze_crawler: str,
        offer_share_crawler: str
) -> str:
    audience_builder_step = GlueStartJobRunStep(
        state_id=f"Run-{audience_builder_job}"[0:80],
        wait_for_completion=True,
        parameters={
            'JobName': audience_builder_job
        }
    )

    braze_push_step = GlueStartJobRunStep(
        state_id=f"Run-{braze_push_job}"[0:80],
        wait_for_completion=True,
        parameters={
            'JobName': braze_push_job
        }
    )
    crawl_audience_step = run_crawler(audience_crawler)
    crawl_audiencebraze_step = run_crawler(audience_braze_crawler)
    crawl_audience_array_step = run_crawler(audience_array_crawler)
    crawl_audience_payload_step = run_crawler(audience_payload_crawler)
    crawl_deltamapping_step = run_crawler(delta_mapping_crawler)
    crawl_offerbraze_step = run_crawler(offer_braze_crawler)
    crawl_offershare_step = run_crawler(offer_share_crawler)

    # offer_builder_job = GlueStartJobRunStep(
    #     state_id=f"Run-{offer_builder_job}"[0:80],
    #     wait_for_completion=True,
    #     parameters={
    #         'JobName': audience_builder_job
    #     }
    # )

    flow = Workflow(
        name="cvm-braze-audience-workflow",
        role=workflow_execution_role_arn,
        definition=Chain([
            audience_builder_step,
            braze_push_step,
            crawl_audience_step,
            crawl_audiencebraze_step,
            crawl_audience_array_step,
            crawl_audience_payload_step,
            crawl_deltamapping_step,
            crawl_offerbraze_step,
            crawl_offershare_step,
            Succeed(state_id=f"Success-braze-new-audience-flow")
        ])
    )

    return flow.definition.to_json(pretty=True)
