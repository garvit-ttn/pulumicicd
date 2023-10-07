import pulumi
import pulumi_aws as aws
from pulumi import Input

from cvmdatalake.pulumi import migrate_to_terraform_opts


def provision(vpc_id: Input[str], environment: str):
    sm_policy_document = aws.iam.get_policy_document(statements=[aws.iam.GetPolicyDocumentStatementArgs(
        actions=["sts:AssumeRole"],
        principals=[
            aws.iam.GetPolicyDocumentStatementPrincipalArgs(
                type="Service",
                identifiers=["sagemaker.amazonaws.com"],
            ),
            # aws.iam.GetPolicyDocumentStatementPrincipalArgs(
            #     type="AWS",
            #     identifiers=["arn:aws:iam::*:role/cvm_*_glue_execution_role*"],
            # )
        ],
    )])

    execution_role = aws.iam.Role(
        "cvm-sagemaker-execution-role",
        path="/",
        assume_role_policy=sm_policy_document.json,
        opts=migrate_to_terraform_opts(environment)
    )

    execution_permissions = aws.iam.get_policy_document(statements=[
        aws.iam.GetPolicyDocumentStatementArgs(
            effect="Allow",
            actions=["*"],
            resources=["*"],
        ),
    ])

    aws.iam.RolePolicy(
        "cvm-sagemaker-execution-policy",
        role=execution_role.id,
        policy=execution_permissions.json,
        opts=migrate_to_terraform_opts(environment)
    )

    subnets = _provision_network(vpc_id, environment)

    key_alias_name = 'cvm-sagemaker-2' if environment == 'dev' else 'cvm-sagemaker'

    efs_key = aws.kms.Key('cvm-sagemaker-efs-key', aws.kms.KeyArgs(), opts=migrate_to_terraform_opts(environment))
    aws.kms.Alias('cvm-sagemaker-efs-key-alias', aws.kms.AliasArgs(
        target_key_id=efs_key.key_id,
        name=f'alias/{key_alias_name}',
    ), opts=migrate_to_terraform_opts(environment))

    # pulumi.export('base.sm.exec_role_arn', execution_role.arn)

    return aws.sagemaker.Domain("cvm-sagemaker-domain", aws.sagemaker.DomainArgs(
        domain_name="cvm-sagemaker-domain",
        vpc_id=vpc_id,
        subnet_ids=subnets,
        auth_mode="IAM",
        kms_key_id=efs_key.arn,
        default_user_settings=aws.sagemaker.DomainDefaultUserSettingsArgs(
            execution_role=execution_role.arn
        )
    ), opts=migrate_to_terraform_opts(environment))


def _provision_network(vpc_id: Input[str], environment: str):
    config = pulumi.Config()
    subnets: dict = config.require_object('sagemaker_subnets')

    sm_subnet_ids = []

    for zone, addr in subnets.items():
        subnet_name = f"SM-Subnet-{zone}"
        subnet = aws.ec2.Subnet(subnet_name, args=aws.ec2.SubnetArgs(
            vpc_id=vpc_id,
            cidr_block=addr,
            availability_zone=f"eu-west-1{zone}",
        ), opts=migrate_to_terraform_opts(environment))

        aws.ec2.Tag(
            f"sm-subnet-{zone}-nameTag",
            resource_id=subnet.id,
            key="Name",
            value=subnet_name,
            opts=migrate_to_terraform_opts(environment)
        )

        sm_subnet_ids.append(subnet.id)

    return sm_subnet_ids
