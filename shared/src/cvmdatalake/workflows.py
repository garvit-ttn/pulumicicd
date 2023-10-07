from functools import partial
from inspect import getfullargspec
from typing import Any, Sequence, Callable

from pulumi import Input, Output
from stepfunctions.steps import ChoiceRule, Choice, Wait, Task
from stepfunctions.steps.states import Chain

WorkflowLiftArgsType = Input[Any] | Sequence[Input[Any]] | Any
WorkflowGenerator = Callable[[str], str]


def workflow_lift(gen_fun: Callable[[...], str], **args: WorkflowLiftArgsType) -> Output[WorkflowGenerator]:
    gen_fun_params = set(getfullargspec(gen_fun).args)
    gen_fun_params.remove('workflow_execution_role_arn')  # Default first param for every workflow generation function

    lift_args = set(args.keys())

    if gen_fun_params != lift_args:
        raise Exception(
            f"""
            There's is a missmatch of arguments in workflow generation function: {gen_fun.__qualname__}
            Required Generation Function arguments: {gen_fun_params}
            Provided lift arguments: {lift_args}       
            """
        )

    # Converts Sequence[Output[T]] into Output[Sequence[T]. Output[T] and ordinary types are untouched
    gen_fun_args = {
        k: Output.all(*v) if isinstance(v, Sequence) and all(isinstance(i, Output) for i in v) else v
        for k, v in args.items()
    }

    return Output.all(**gen_fun_args).apply(lambda _: partial(gen_fun, **_))


def run_crawler(
        crawler_name: str,
        # next_step: Optional[State | Chain] = None,
        wait_for_results_seconds: int = 30,
        invocation_identifier: str = 'a',
) -> Chain:
    start_crawler = Task(
        state_id=f"Start-{crawler_name}-{invocation_identifier}",
        resource="arn:aws:states:::aws-sdk:glue:startCrawler",
        parameters={
            'Name': crawler_name
        }
    )

    get_crawler_status = Task(
        state_id=f"GetStatus-{crawler_name}-{invocation_identifier}",
        resource="arn:aws:states:::aws-sdk:glue:getCrawler",
        parameters={
            'Name': crawler_name
        }
    )

    wait_for_crawler = Wait(
        state_id=f"Wait-{crawler_name}-{invocation_identifier}",
        seconds=wait_for_results_seconds
    )

    # if next_step is None:
    #     next_step = Succeed(state_id=f"Success-{crawler_name}-{invocation_identifier}")

    check_crawler_status = Choice(state_id=f"CheckStatus-{crawler_name}-{invocation_identifier}")
    # check_crawler_status.default_choice(next_step)

    check_crawler_status.add_choice(
        rule=ChoiceRule.StringEquals(variable='$.Crawler.State', value='RUNNING'),
        next_step=wait_for_crawler
    )

    # We don't need to way till crawler is full stopped.
    # It only prevents us from running the same crawler again, but the results are already available in Catalog
    # check_crawler_status.add_choice(
    #     rule=ChoiceRule.StringEquals(variable='$.Crawler.State', value='STOPPING'),
    #     next_step=wait_for_crawler
    # )

    return Chain([start_crawler, wait_for_crawler, get_crawler_status, check_crawler_status])
