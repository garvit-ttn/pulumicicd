from typing import List

import pulumi
import pulumi_aws as aws
from pulumi import Input, ResourceOptions


def provision_instance(vpc_id: Input[str], db_subnets_ids: List[Input[str]], security_group_id: Input[str]):
    dms_assume_role = aws.iam.get_policy_document(statements=[aws.iam.GetPolicyDocumentStatementArgs(
        actions=["sts:AssumeRole"],
        principals=[aws.iam.GetPolicyDocumentStatementPrincipalArgs(
            identifiers=["dms.amazonaws.com"],
            type="Service",
        )],
    )])

    dms_access_for_endpoint = aws.iam.Role("cvm-dms-access-for-endpoint", assume_role_policy=dms_assume_role.json)
    dms_access_for_endpoint_amazon_dms_redshift_s3_role = aws.iam.RolePolicyAttachment(
        "cvm-dms-access-for-endpoint-AmazonDMSRedshiftS3Role",
        policy_arn="arn:aws:iam::aws:policy/service-role/AmazonDMSRedshiftS3Role",
        role=dms_access_for_endpoint.name
    )

    dms_cloudwatch_logs_role = aws.iam.Role("cvm-dms-cloudwatch-logs-role", assume_role_policy=dms_assume_role.json)
    dms_cloudwatch_logs_role_amazon_dms_cloud_watch_logs_role = aws.iam.RolePolicyAttachment(
        "cvm-dms-cloudwatch-logs-role-AmazonDMSCloudWatchLogsRole",
        policy_arn="arn:aws:iam::aws:policy/service-role/AmazonDMSCloudWatchLogsRole",
        role=dms_cloudwatch_logs_role.name
    )

    dms_vpc_role = aws.iam.Role("cvm-dms-vpc-role", assume_role_policy=dms_assume_role.json)
    dms_vpc_role_amazon_vpc_management_role = aws.iam.RolePolicyAttachment(
        "cvm-dms-vpc-role-AmazonDMSVPCManagementRole",
        policy_arn="arn:aws:iam::aws:policy/service-role/AmazonDMSVPCManagementRole",
        role=dms_vpc_role.name
    )

    aws.ec2.get_subnets()

    dms_subnet_group = aws.dms.ReplicationSubnetGroup(
        "cvm-dms-subnet-group",
        aws.dms.ReplicationSubnetGroupArgs(
            replication_subnet_group_description="CVM Data Migration Services Subnet Group",
            replication_subnet_group_id="cvm-dms-subnet-group",
            subnet_ids=db_subnets_ids,
        ))

    dms_instance = aws.dms.ReplicationInstance("cvm-data-migration-instance", aws.dms.ReplicationInstanceArgs(
        allocated_storage=20,
        apply_immediately=True,
        auto_minor_version_upgrade=True,
        engine_version="3.4.7",
        multi_az=True,
        preferred_maintenance_window="sun:10:30-sun:14:30",
        publicly_accessible=False,
        replication_instance_class="dms.t2.micro",
        replication_instance_id="cvm-data-migration-instance",
        replication_subnet_group_id=dms_subnet_group.id,
        vpc_security_group_ids=[security_group_id],
    ), opts=ResourceOptions(depends_on=[
        dms_access_for_endpoint_amazon_dms_redshift_s3_role,
        dms_cloudwatch_logs_role_amazon_dms_cloud_watch_logs_role,
        dms_vpc_role_amazon_vpc_management_role,
    ]))

    config = pulumi.Config()

    rds_endpoint = aws.dms.Endpoint(
        "cvm-dms-offer-management-db-replica-endpoint",
        aws.dms.EndpointArgs(
            endpoint_id="cvm-dms-offer-management-db-replica-endpoint",
            endpoint_type="source",
            engine_name="mariadb",
            server_name=config.require_secret('rds_host'),
            port=3306,
            ssl_mode="none",
            username='backend',
            password=config.require_secret('cvm-offers-mgmt-db-password'),
        ))

    pulumi.export('base.dms.replication_instance_arn', dms_instance.replication_instance_arn)
    pulumi.export('base.dms.rds_endpoint_arn', rds_endpoint.endpoint_arn)
