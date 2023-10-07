from pyspark.pandas import DataFrame
from delta.tables import DeltaTable

from cvmdatalake import create_delta_table_from_catalog, conformed


def update_set(cols: list[str]) -> dict[str, str]:
    return {f"target.{c}": f"source.{c}" for c in cols}


def merge_profiles(target: DeltaTable, source: DataFrame):
    target.alias('target').merge(
        source.alias('source'),
        f"""
        source.{conformed.CustomerProfile.idi_counterparty} = target.{conformed.CustomerProfile.idi_counterparty} AND
        source.{conformed.CustomerProfile.bu} = target.{conformed.CustomerProfile.bu}
        """
    ).whenMatchedUpdate(set=update_set([
        conformed.CustomerProfile.frequency,
        conformed.CustomerProfile.monetary,
        conformed.CustomerProfile.customer_age,
        conformed.CustomerProfile.expected_purchases_3m,
        conformed.CustomerProfile.clv_3m,
        conformed.CustomerProfile.p_alive,
        conformed.CustomerProfile.ni_locations
    ])).execute()


if __name__ == '__main__':
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from delta.tables import DeltaTable
    from pyspark.sql.functions import *

    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'lake_descriptor', 'cvm_environment', 'data_filter_context'])

    sc = SparkContext()
    glue_context = GlueContext(sc)
    job = Job(glue_context)
    job.init(args['JOB_NAME'], args)

    spark = glue_context.spark_session
    lake_descriptor = args['lake_descriptor']
    cvm_environment = args['cvm_environment']
    data_filter_context = args['data_filter_context']

    customer_profile_delta = create_delta_table_from_catalog(spark, conformed.CustomerProfile, lake_descriptor)

    profiles_from_prod_df = DeltaTable.forPath(
        spark, "s3://cvm-uat-conformed-d5b175d/customer_profile_from_prod/").toDF()

    merge_profiles(customer_profile_delta, profiles_from_prod_df)
