import pulumi_aws as aws
from pulumi import StackReference

from deploy import glue_exec_role


def provision(lake_stack: StackReference, base_stack: StackReference, prefix: str):
    s3_endpoint = aws.dms.S3Endpoint(f"cvm_{prefix}_dms_offer_management_landing_endpoint", aws.dms.S3EndpointArgs(
        endpoint_id=f"cvm-{prefix}-dms-offer-management-landing-endpoint",
        endpoint_type="target",
        bucket_name=lake_stack.get_output("landing.bucket_id"),
        bucket_folder='cvm-frontend-export',
        data_format='parquet',
        # csv_delimiter=",",
        # csv_row_delimiter='\\n',
        # ignore_header_rows=1,
        date_partition_enabled=False,
        service_access_role_arn=glue_exec_role.arn,
    ))

    # TODO: fix error with replicationTaskSetting being removed
    aws.dms.ReplicationTask(f"cvm_{prefix}_dms_offer_management_to_landing_task", aws.dms.ReplicationTaskArgs(
        replication_task_id=f"cvm-{prefix}-dms-offer-management-to-landing-task",
        migration_type="full-load",
        # cdc_start_time="1484346880",
        start_replication_task=False,
        replication_instance_arn=base_stack.get_output("base.dms.replication_instance_arn"),
        source_endpoint_arn=base_stack.get_output("base.dms.rds_endpoint_arn"),
        target_endpoint_arn=s3_endpoint.endpoint_arn,
        table_mappings="""
        {
          "rules": [
            {
              "rule-type": "selection",
              "rule-id": "123567",
              "rule-name": "123567",
              "object-locator": {
                "schema-name": "%",
                "table-name": "offers"
              },
              "rule-action": "include",
              "filters": []
            },
            {
              "rule-type": "selection",
              "rule-id": "67894564",
              "rule-name": "67894564",
              "object-locator": {
                "schema-name": "%",
                "table-name": "offer_insentive_products"
              },
              "rule-action": "include",
              "filters": []
            },
            {
              "rule-type": "selection",
              "rule-id": "35464756",
              "rule-name": "35464756",
              "object-locator": {
                "schema-name": "%",
                "table-name": "offer_conditional_products"
              },
              "rule-action": "include",
              "filters": []
            },
            {
              "rule-type": "selection",
              "rule-id": "678567453",
              "rule-name": "678567453",
              "object-locator": {
                "schema-name": "%",
                "table-name": "cdp_audience_information"
              },
              "rule-action": "include",
              "filters": []
            }    
          ]
        }
        """,
    ))
