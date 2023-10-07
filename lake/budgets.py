import pulumi_aws as aws

exsell_notification_emails = [
    "krzysztof.sadowski@exsell.nl"
]

maf_notification_emails = [
    "Krzysztof.Sadowski-e@maf.ae"
]

_env_budget = {
    'dev': "3000",
    'uat': "3000",
    'prod': "10000",
}


def provision(environment: str):
    notification_emails = exsell_notification_emails + maf_notification_emails

    aws.budgets.Budget(f"cvm-{environment}-total-budget", aws.budgets.BudgetArgs(
        budget_type="COST",
        limit_unit="USD",
        time_unit="MONTHLY",
        limit_amount=_env_budget.get(environment, 100),
        time_period_start="2022-09-01_00:00",
        cost_filters=[
            aws.budgets.BudgetCostFilterArgs(
                name="Region",
                values=["eu-west-1"],
            )
        ],
        notifications=[
            aws.budgets.BudgetNotificationArgs(
                comparison_operator="GREATER_THAN",
                notification_type="ACTUAL",
                subscriber_email_addresses=notification_emails,
                threshold=50,
                threshold_type="PERCENTAGE",
            ),
            aws.budgets.BudgetNotificationArgs(
                comparison_operator="GREATER_THAN",
                notification_type="ACTUAL",
                subscriber_email_addresses=notification_emails,
                threshold=80,
                threshold_type="PERCENTAGE",
            ),
            aws.budgets.BudgetNotificationArgs(
                comparison_operator="GREATER_THAN",
                notification_type="ACTUAL",
                subscriber_email_addresses=notification_emails,
                threshold=90,
                threshold_type="PERCENTAGE",
            ),
            aws.budgets.BudgetNotificationArgs(
                comparison_operator="GREATER_THAN",
                notification_type="ACTUAL",
                subscriber_email_addresses=notification_emails,
                threshold=95,
                threshold_type="PERCENTAGE",
            ),
        ],
    ))
