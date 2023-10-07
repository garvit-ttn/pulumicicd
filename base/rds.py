import pulumi
import pulumi_aws as aws


def provision():
    vpc = aws.ec2.Vpc("rds-vpc", aws.ec2.VpcArgs(
        cidr_block="10.200.0.0/16",
        enable_dns_support=True,
        enable_dns_hostnames=True
    ))

    main_subnet = aws.ec2.Subnet("main-subnet", aws.ec2.SubnetArgs(
        vpc_id=vpc.id,
        cidr_block="10.200.20.0/24",
        availability_zone=f"eu-west-1a",
        tags={
            "Name": "main-subnet-1a"
        }
    ))

    read_subnet = aws.ec2.Subnet("read-subnet", aws.ec2.SubnetArgs(
        vpc_id=vpc.id,
        cidr_block="10.200.21.0/24",
        availability_zone=f"eu-west-1b",
        tags={
            "Name": "read-subnet-1b"
        }
    ))

    read_subnet2 = aws.ec2.Subnet("read2-subnet", aws.ec2.SubnetArgs(
        vpc_id=vpc.id,
        cidr_block="10.200.22.0/24",
        availability_zone=f"eu-west-1c",
        tags={
            "Name": "read-subnet-1c"
        }
    ))

    # Route to VPC (local) is added automatically by default
    private_route_table = aws.ec2.RouteTable("private-route-table", aws.ec2.RouteTableArgs(
        vpc_id=vpc.id,
        tags={
            "Name": "rds-private-route-table"
        }
    ))

    aws.ec2.RouteTableAssociation("private-route-table-association-with-1a", aws.ec2.RouteTableAssociationArgs(
        route_table_id=private_route_table.id,
        subnet_id=main_subnet.id
    ))

    aws.ec2.RouteTableAssociation("private-route-table-association-with-1b", aws.ec2.RouteTableAssociationArgs(
        route_table_id=private_route_table.id,
        subnet_id=read_subnet.id
    ))

    aws.ec2.RouteTableAssociation("private-route-table-association-with-1c", aws.ec2.RouteTableAssociationArgs(
        route_table_id=private_route_table.id,
        subnet_id=read_subnet2.id
    ))

    s3_gw_endpoint_policy = aws.iam.get_policy_document(statements=[aws.iam.GetPolicyDocumentStatementArgs(
        sid="Access-to-s3-buckets",
        effect="Allow",
        actions=["*"],
        resources=["arn:aws:s3:::*"],
        principals=[aws.iam.GetPolicyDocumentStatementPrincipalArgs(
            type="*",
            identifiers=["*"],
        )],
    )])

    current_region = aws.get_region()
    aws.ec2.VpcEndpoint(f"s3-endpoint", aws.ec2.VpcEndpointArgs(
        vpc_endpoint_type="Gateway",
        vpc_id=vpc.id,
        service_name=f"com.amazonaws.{current_region.name}.s3",
        route_table_ids=[private_route_table.id],
        policy=s3_gw_endpoint_policy.json,
    ))

    subnet_group = aws.rds.SubnetGroup("testInstance-sn-group", aws.rds.SubnetGroupArgs(
        name="test-cluster-subnet-group",
        subnet_ids=[main_subnet.id, read_subnet.id, read_subnet2.id]
    ))

    aws.rds.Instance("testInstance", aws.rds.InstanceArgs(
        allocated_storage=10,
        identifier="mydb",
        engine="postgres",
        engine_version="13.7",
        instance_class="db.t3.micro",
        db_name="mydb",
        username="adminor",
        password="asdqwe345",
        backup_retention_period=7,
        storage_encrypted=True,
        skip_final_snapshot=True,
        auto_minor_version_upgrade=True,
        db_subnet_group_name=subnet_group.name,
        multi_az=True
    ))

    # zones = aws.get_availability_zones(state="available")
    # for z in zones.names:
    #     print(f"{z}")

    # aws.rds.Cluster("multiaz-cluster-example", aws.rds.ClusterArgs(
    #     cluster_identifier="multiaz-cluster-example",
    #     availability_zones=zones.names,
    #     master_username="test",
    #     master_password="mustbeeightcharaters",
    #     db_cluster_instance_class="db.m5d.large",
    #     engine="postgres",
    #     engine_version="13.7",
    #     iops=1000,
    #     storage_type="io1",
    #     allocated_storage=100,
    #     db_subnet_group_name=subnet_group.name,
    #     database_name="my_db",
    #     skip_final_snapshot=True,
    # ))
