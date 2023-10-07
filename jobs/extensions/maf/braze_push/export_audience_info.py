from cvmdatalake import get_s3_path, landing
from pyspark.sql.functions import *

if __name__ == '__main__':
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions

    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'lake_descriptor', 'cvm_environment', 'rds_cvm_connection_name'])

    sc = SparkContext()
    glueContext = GlueContext(sc)
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    spark = glueContext.spark_session

    lake_descriptor = args['lake_descriptor']
    environment = args['cvm_environment']
    rds_connection = args['rds_cvm_connection_name']

    audience_info: DataFrame = glueContext.create_dynamic_frame_from_options(
        connection_type='mysql',
        connection_options={
            "useConnectionProperties": "true",
            "dbtable": 'cdp_audience_information',
            "connectionName": rds_connection,
        }
    ).toDF()

    campaigns: DataFrame = glueContext.create_dynamic_frame_from_options(
        connection_type='mysql',
        connection_options={
            "useConnectionProperties": "true",
            "dbtable": 'offerbank_campaigns',
            "connectionName": rds_connection,
        }
    ).toDF()

    offers_bank_offers: DataFrame = glueContext.create_dynamic_frame_from_options(
        connection_type='mysql',
        connection_options={
            "useConnectionProperties": "true",
            "dbtable": 'offerbank_offers',
            "connectionName": rds_connection,
        }
    ).toDF()

    audience_info.coalesce(1).write.format('parquet').mode('overwrite').save(
        get_s3_path(landing.AudienceInfo, lake_descriptor)
    )

    campaigns.coalesce(1).write.format('parquet').mode('overwrite').save(
        get_s3_path(landing.Campaign, lake_descriptor)
    )

    offers_bank_offers.coalesce(1).write.format('parquet').mode('overwrite').save(
        get_s3_path(landing.Offer, lake_descriptor)
    )
