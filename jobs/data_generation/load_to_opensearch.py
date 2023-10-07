import json
from datetime import datetime, date
from io import StringIO
from typing import Iterator, TypeVar

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from opensearchpy import AWSV4SignerAuth, OpenSearch, RequestsHttpConnection
from pyspark.sql import Row
from pyspark.sql.functions import *

from cvmdatalake import conformed


def open_search_client():
    region = 'eu-west-1'
    host = f'z0lb5uunk9v6r5o11fj1.{region}.aoss.amazonaws.com'
    service = 'aoss'
    credentials = boto3.Session().get_credentials()
    auth = AWSV4SignerAuth(credentials, region, service)

    return OpenSearch(
        hosts=[{'host': host, 'port': 443}],
        http_auth=auth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection,
        pool_maxsize=1,
        timeout=120
    )


def add_to_open_search(partition: Iterator[Row]):
    def json_serializer(obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        raise TypeError(f'Type {type(obj)} is not serializable')

    t_batch_element = TypeVar("t_batch_element")

    def batched(seq: list[t_batch_element], batch_size: int) -> list[list[t_batch_element]]:
        return list((seq[i:i + batch_size] for i in range(0, len(seq), batch_size)))

    id_col_name = conformed.CustomerProfile.idi_counterparty.name
    index_batches = batched(list(partition), 1000)

    for batch in index_batches:
        body = StringIO()

        for item in batch:
            body.write(json.dumps({"index": {"_id": item[id_col_name]}}))
            body.write('\n')

            body.write(json.dumps(item.asDict(), default=json_serializer))
            body.write('\n')

        os_client = open_search_client()
        response = os_client.bulk(
            index=f"sample_profile_{sample_dataset.lower()}",
            body=body.getvalue()
        )

        print(response)


args = getResolvedOptions(
    sys.argv,
    ['JOB_NAME', 'lake_descriptor', 'cvm_environment', 'sample_dataset']
)

sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
spark = glueContext.spark_session

lake_descriptor = args['lake_descriptor']
environment = args['cvm_environment']

sample_dataset: str = args['sample_dataset']

sample = spark.read.format('delta').load(f"s3://cvm-krz-conformed-7a4f071/sample_profiles/{sample_dataset}")
sample.show(truncate=False)

sample.foreachPartition(add_to_open_search)
