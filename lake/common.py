import pulumi_aws as aws


def cvm_crawler_role(prefix: str) -> aws.iam.Role:
    assume_policy = aws.iam.get_policy_document(statements=[aws.iam.GetPolicyDocumentStatementArgs(
        actions=["sts:AssumeRole"],
        effect="Allow",
        principals=[aws.iam.GetPolicyDocumentStatementPrincipalArgs(
            type="Service",
            identifiers=["glue.amazonaws.com"],
        )],
    )])

    role = aws.iam.Role(
        f"cvm_crawler_{prefix}_execution-role",
        path="/",
        assume_role_policy=assume_policy.json
    )

    # Here's where we should specify fine-grained actions that airflow need while accessing other AWS services
    permissions = aws.iam.get_policy_document(statements=[
        aws.iam.GetPolicyDocumentStatementArgs(
            effect="Allow",
            resources=["*"],
            actions=[
                "glue:*",
                "s3:GetBucketLocation",
                "s3:ListBucket",
                "s3:ListAllMyBuckets",
                "s3:GetBucketAcl",
                "ec2:DescribeVpcEndpoints",
                "ec2:DescribeRouteTables",
                "ec2:CreateNetworkInterface",
                "ec2:DeleteNetworkInterface",
                "ec2:DescribeNetworkInterfaces",
                "ec2:DescribeSecurityGroups",
                "ec2:DescribeSubnets",
                "ec2:DescribeVpcAttribute",
                "iam:ListRolePolicies",
                "iam:GetRole",
                "iam:GetRolePolicy",
                "cloudwatch:PutMetricData"
            ]
        ),
        aws.iam.GetPolicyDocumentStatementArgs(
            effect="Allow",
            resources=["arn:aws:s3:::aws-glue-*"],
            actions=[
                "s3:CreateBucket",
                "s3:PutBucketPublicAccessBlock"
            ],
        ),
        aws.iam.GetPolicyDocumentStatementArgs(
            effect="Allow",
            resources=[
                "arn:aws:s3:::aws-glue-*/*",
                "arn:aws:s3:::*/*aws-glue-*/*"
            ],
            actions=[
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject"
            ],
        ),
        aws.iam.GetPolicyDocumentStatementArgs(
            effect="Allow",
            resources=[
                "arn:aws:s3:::cvm-*",
            ],
            actions=[
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject"
            ],
        ),
        aws.iam.GetPolicyDocumentStatementArgs(
            effect="Allow",
            resources=[
                "arn:aws:s3:::crawler-public*",
                "arn:aws:s3:::aws-glue-*"
            ],
            actions=[
                "s3:GetObject"
            ],
        ),
        aws.iam.GetPolicyDocumentStatementArgs(
            effect="Allow",
            resources=[
                "arn:aws:logs:*:*:/aws-glue/*"
            ],
            actions=[
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents",
                "logs:AssociateKmsKey",
            ],
        ),
        aws.iam.GetPolicyDocumentStatementArgs(
            effect="Allow",
            resources=[
                "arn:aws:ec2:*:*:network-interface/*",
                "arn:aws:ec2:*:*:security-group/*",
                "arn:aws:ec2:*:*:instance/*",
            ],
            actions=[
                "ec2:CreateTags",
                "ec2:DeleteTags"
            ],
            conditions=[
                aws.iam.GetPolicyDocumentStatementConditionArgs(
                    test="ForAllValues:StringEquals",
                    variable="aws:TagKeys",
                    values=["aws-glue-service-resource"]
                )
            ]
        ),
    ])

    aws.iam.RolePolicy(
        f"cvm-crawler{prefix}-execution-policy",
        role=role.id,
        policy=permissions.json
    )

    return role
