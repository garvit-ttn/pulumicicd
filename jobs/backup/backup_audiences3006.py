import json
import logging
from collections import namedtuple
from functools import partial
from typing import Sized

import requests
from pyspark.sql.types import Row
from pyspark.sql.functions import *

from cvmdatalake import conformed, create_delta_table_from_catalog, creat_delta_table_if_not_exists, get_s3_path
from cvmdatalake.security import get_secret


def map_app_group(cvm_app_group: str) -> list[str]:
    if cvm_app_group == '':
        return []

    cvm_app_groups = cvm_app_group.split(sep='+')
    cvm_app_groups = list(map(lambda ag: ag if ag != 'SHARE' else "LYL", cvm_app_groups))
    return cvm_app_groups if len(cvm_app_groups) <= 1 else ["XBU"]


BrazeInput = namedtuple('BrazeInput', ['business_unit', 'cvm_audience', 'external_id'])
MAX_BRAZE_AUDIENCES_IN_SINGLE_PAYLOAD = 25
BRAZE_ATTRIBUTES_BATCH_SIZE = 75


def send_to_braze(
        session, braze_input: list[BrazeInput],
        braze_sync_api_key: str, braze_sync_auth_url: str, braze_sync_push_url: str
) -> tuple[str, str]:
    try:
        headers = {'Authorization': f'Basic {braze_sync_api_key}'}
        response = session.get(
            braze_sync_auth_url,
            headers=headers
        )
        access_token = response.json()["access_token"]

    except Exception as e:
        return 'Authentication error', str(e)

    attributes = [{
        "appgroup": map_app_group(bri.business_unit),
        "cvm_audience": bri.cvm_audience[:MAX_BRAZE_AUDIENCES_IN_SINGLE_PAYLOAD],
        "external_id": bri.external_id,
    } for bri in braze_input]

    payload = json.dumps({"attributes": attributes})

    try:
        headers = {'Authorization': f'Bearer {access_token}', 'Content-Type': 'application/json'}
        response = session.post(braze_sync_push_url, data=payload, headers=headers)
        status_code = str(response.status_code).strip() if response is not None and response.status_code == 200 else ''

        return payload, status_code

    except Exception as e:
        return payload, str(e)


def batched(seq: Sized, batch_size: int) -> list[list]:
    return list((seq[i:i + batch_size] for i in range(0, len(seq), batch_size)))


def run(audiences: DataFrame, logger, braze_sync_api_key: str, braze_sync_auth_url: str,
        braze_sync_push_url: str) -> DataFrame:
    if conformed.Audiences.campaign_end_date.name in audiences.columns:
        audiences = audiences.filter(col(conformed.Audiences.campaign_end_date.name) >= current_date())

    audiences = audiences.orderBy(desc(col('ingestion_timestamp')))

    print("1")
    audiences.show()
    audiences = (
        audiences.groupBy([conformed.Audiences.external_id.name, conformed.Audiences.business_unit.name])
        .agg(collect_set(audiences.audience).alias("cvm_audience")).withColumn("ingestion_timestamp",
                                                                               current_timestamp()).select(
            'external_id', 'business_unit', 'cvm_audience', 'ingestion_timestamp')
    )
    audiences.show(50, truncate=False)
    audiences.printSchema()

    creat_delta_table_if_not_exists(spark, conformed.AudienceArray, lake_descriptor)
    audience_path1 = get_s3_path(conformed.AudienceArray, lake_descriptor)
    audiences.write.format('delta').mode('overwrite').save(audience_path1)

    print("2")

    logger.info(f"Pushing audiences for {audiences.count()} unique (external_id, business_unit) pairs")

    def send_to_braze_over_partitions(partition: Iterable[Row]):
        session = requests.session()
        batches = batched(list(partition), BRAZE_ATTRIBUTES_BATCH_SIZE)

        for batch in batches:
            payload, status = send_to_braze(
                session, batch, braze_sync_api_key, braze_sync_auth_url, braze_sync_push_url
            )
            yield [payload, status]

        session.close()

    audiences = audiences.coalesce(1)
    audiences.show()
    result = audiences.rdd.mapPartitions(send_to_braze_over_partitions)
    result = result.toDF(['payload', 'result'])
    result = result

    result.cache()
    result.show(10, truncate=False)

    failed = result.filter(col('result') != '200')
    success = result.filter(col('result') == '200')

    logger.info(f"Total requests: {result.count()}")
    logger.info(f"Requests failed: {failed.count()}")
    logger.info(f"Requests success: {success.count()}")

    failed.show(50, truncate=False)
    return result


if __name__ == '__main__':
    from awsglue.context import GlueContext
    from pyspark.context import SparkContext

    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions

    args = getResolvedOptions(sys.argv, [
        'JOB_NAME', 'lake_descriptor', 'cvm_environment',
        'cvm_maf_braze_sync_api_key_secret_name',
        'cvm_maf_braze_sync_api_auth_url',
        'cvm_maf_braze_sync_api_push_url',
    ])

    sc = SparkContext()
    glueContext = GlueContext(sc)
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    spark = glueContext.spark_session
    glue_logger = glueContext.get_logger()
    # glue_logger.addHandler(logging.StreamHandler(sys.stdout))

    lake_descriptor = args['lake_descriptor']
    cvm_environment = args['cvm_environment']

    cvm_maf_braze_sync_auth_url = args['cvm_maf_braze_sync_api_auth_url']
    cvm_maf_braze_sync_push_url = args['cvm_maf_braze_sync_api_push_url']
    cvm_maf_braze_sync_api_key_secret_name = args['cvm_maf_braze_sync_api_key_secret_name']
    cvm_maf_braze_sync_api_key = get_secret(cvm_maf_braze_sync_api_key_secret_name)

    glue_logger.info(f"sync auth url: {cvm_maf_braze_sync_auth_url}")
    glue_logger.info(f"sync push url: {cvm_maf_braze_sync_push_url}")

    audience_df: DataFrame = create_delta_table_from_catalog(spark, conformed.Audiences, lake_descriptor).toDF()
    audience_df.show(1)
    result = run(
        audience_df, glue_logger, cvm_maf_braze_sync_api_key, cvm_maf_braze_sync_auth_url, cvm_maf_braze_sync_push_url
    )

    # Save payload and responses as Delta Table for future reference and inspection
    creat_delta_table_if_not_exists(spark, conformed.AudiencePayload, lake_descriptor)
    audience_path = get_s3_path(conformed.AudiencePayload, lake_descriptor)
    result.write.format('delta').mode('overwrite').save(audience_path)
