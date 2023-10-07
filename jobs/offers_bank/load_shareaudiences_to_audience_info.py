import json

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, IntegerType, DateType

from cvmdatalake import create_delta_table_from_catalog
from cvmdatalake.conformed import Promotions
from cvmdatalake.security import get_secret
from pyspark.sql import functions as F
from cvmdatalake import conformed, create_delta_table_from_catalog, creat_delta_table_if_not_exists, get_s3_path

empty_valid_json = json.dumps({})

#
# def map_share_audiences_to_audience_overview(promo: DataFrame) -> DataFrame:
#     mapped_offers = (
#         promo.filter(
#             (lower(Promotions.des_restrictions.name) == 'all') &
#             (col(Promotions.dat_period_end.name) > current_date()) | col(Promotions.dat_period_end.name).isNull()
#         )
#         .withColumn("created_at", current_date())
#         .withColumn("offer_type_id", col(Promotions.des_offer_type.name))
#         .withColumn("offer_type", col(Promotions.des_offer_type.name))
#         .withColumn("unit", lit("").cast(StringType()))
#         .withColumn("restriction", col(Promotions.des_restrictions.name))
#         .withColumn("restriction_type", col(Promotions.des_restrictions.name))
#         .withColumn("budget", lit("").cast(StringType()))
#         .withColumn("offer_startdate", col(Promotions.dat_period_start.name))
#         .withColumn("offer_enddate", col(Promotions.dat_period_end.name))
#         .withColumn("locations", col(Promotions.des_online_ofline.name))
#         .withColumn(
#             "offer_name",
#             format_string("%s (%s)", col(Promotions.nam_offer.name), col(Promotions.idi_offer.name))
#         )
#         .withColumn("business_unit", col(Promotions.cde_bu.name))
#         .withColumn("description", col(Promotions.des_offer.name))
#         .withColumn("image_link", col(Promotions.des_url_image.name))
#         .withColumn("image_tag", lit("").cast(StringType()))
#         .withColumn("value", col(Promotions.des_offer_value.name))
#         .withColumn("draft", lit(0).cast(IntegerType()))
#         .withColumn("active", when((col(Promotions.dat_period_start.name) <= current_date()) & (
#                     col(Promotions.dat_period_end.name) >= current_date()), lit(1).cast(IntegerType())).when(
#             (col(Promotions.dat_period_end.name).isNull()), lit(1).cast(IntegerType())))
#         .withColumn("brand", col(Promotions.des_brand.name))
#         .withColumn("user_id_id", lit(1).cast(IntegerType()))
#         .withColumn("conditional_products_json", lit(empty_valid_json).cast(StringType()))
#         .withColumn("incentive_products_json", lit(empty_valid_json).cast(StringType()))
#         .withColumn("sponsor_name", col(Promotions.nam_sponsor.name))
#     )
#
#     return mapped_offers.drop(*promo.columns)


# promo_columns_mandatory_for_share = [
#     Promotions.idi_offer,
#     Promotions.nam_offer,
#     Promotions.ind_member_visible,
#     Promotions.des_offer,
#     Promotions.des_url_image,
#     Promotions.des_url_mobile_image,
#     Promotions.dat_period_start,
#     # Promotions.dat_period_end,
#     Promotions.des_offer_section,
#     Promotions.des_offer_type,
#     Promotions.des_terms,
#     Promotions.cde_sponsor_key,
#     Promotions.nam_sponsor,
#     Promotions.des_url_logo,
#     Promotions.ind_is_template_based
# ]
def share_aud_info(share_audience: DataFrame):
    share_audience = share_audience


    return share_audience



if __name__ == '__main__':
    from awsglue import DynamicFrame
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.transforms import *
    from awsglue.utils import getResolvedOptions

    args = getResolvedOptions(sys.argv, [
        'JOB_NAME', 'lake_descriptor', 'cvm_environment',
        'rds_cvm_connection_name', 'cvm_rds_db_name', 'cvm_offers_db_password_secret_name'
    ])

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark: SparkSession = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    rds_connection = args['rds_cvm_connection_name']
    lake_descriptor = args['lake_descriptor']
    environment = args['cvm_environment']
    rds_db_name = args['cvm_rds_db_name']

    promotions = create_delta_table_from_catalog(spark, Promotions, lake_descriptor).toDF()

    share_audience = create_delta_table_from_catalog(spark, conformed.ShareAudience, lake_descriptor).toDF()

    audience_info: DataFrame = glueContext.create_dynamic_frame_from_options(
        connection_type='mysql',
        connection_options={
            "useConnectionProperties": "true",
            "dbtable": 'cdp_audience_information',
            "connectionName": rds_connection,
        }
    ).toDF()

    campaigns_df: DataFrame = glueContext.create_dynamic_frame_from_options(
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
            "dbtable": 'offerbank_gravityoffer',
            "connectionName": rds_connection,
        }
    ).toDF()

    share_aud_info = share_aud_info(share_audience )
    share_audience.show(1)