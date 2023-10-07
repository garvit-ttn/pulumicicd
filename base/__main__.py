import pulumi
import pulumi_aws as aws
from pulumi import Output, Input

import maf_sagemaker
from cvmdatalake.pulumi import enable_debug, migrate_to_terraform, migrate_to_terraform_opts


def export_config(name: str):
    pulumi.export(name, config.require(name))


def create_secret(name: Input[str], secret: Input[str], description: Input[str] = '') -> aws.secretsmanager.Secret:
    aws_secret = aws.secretsmanager.Secret(f'cvm-{name}-secret', aws.secretsmanager.SecretArgs(
        description=description,
    ), opts=migrate_to_terraform_opts(environment))

    aws.secretsmanager.SecretVersion(f'cvm-{name}-secret-version', aws.secretsmanager.SecretVersionArgs(
        secret_id=aws_secret.id,
        secret_string=secret
    ), opts=migrate_to_terraform_opts(environment))

    return aws_secret


enable_debug(flag=False)

config = pulumi.Config()

maf_vpc_id = config.require('maf_vpc_id')
environment = config.require('environment')

rds_host = config.require('rds_host')
rds_db_name = config.require('cvm-offers-db-name')
rds_password = config.require_secret('cvm-offers-mgmt-db-password')

# export important config shared in downstream projects
export_config("sm-exec-role-arn")

export_config('cvm-braze-push-auth-url')
export_config('cvm-braze-push-url')

export_config('cvm-offers-db-name')
export_config('rds_host')

export_config('cvm-braze-push-auth-secret_name')
export_config('rds_cvm_connection_name')
export_config('cvm-offers-db-password-secret-name')

if not migrate_to_terraform(environment):
    maf_sagemaker.provision(
        vpc_id=maf_vpc_id,  # default VPC created by MAF
        environment=environment
    )

    braze_push_auth_secret = create_secret(
        name='braze-push-auth',
        secret=config.require_secret('cvm-braze-auth-key'),
        description='Authentication key for MAF Braze Audience Push API'
    )

    braze_push_mtls_cert = create_secret(
        name='braze-push-mtls-cert',
        secret=rds_password,
        description='Client certificate for MTLS authentication to MAF Braze Audience Push API'
    )

    rds_password_secret = create_secret(
        name='rds-password',
        secret=rds_password,
        description='Password to RDS MaariaDB'
    )

    rds_cvm_connection = aws.glue.Connection('cvm-connection-rds', aws.glue.ConnectionArgs(
        connection_type='JDBC',
        connection_properties={
            "USERNAME": "backend",
            "PASSWORD": rds_password,
            "JDBC_CONNECTION_URL": Output.all(host=rds_host, db=rds_db_name).apply(
                lambda x: f"jdbc:mysql://{x['host']}:3306/{x['db']}"
            )
        },
        physical_connection_requirements=aws.glue.ConnectionPhysicalConnectionRequirementsArgs(
            security_group_id_lists=[config.require('rds_security_group')],
            subnet_id=config.require_object('dms_subnets')[0],
            availability_zone='eu-west-1a',
        )
    ), opts=migrate_to_terraform_opts(environment))
