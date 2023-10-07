import json

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, IntegerType, DateType

from cvmdatalake import create_delta_table_from_catalog
from cvmdatalake.conformed import Promotions

empty_valid_json = json.dumps({})


def map_promotions_to_offers(promo: DataFrame) -> DataFrame:
    mapped_offers = (
        promo.filter(
            (lower(Promotions.des_restrictions.name) == 'all') &
            (col(Promotions.dat_period_end.name) > current_date()) | col(Promotions.dat_period_end.name).isNull()
        )
        .withColumn("created_at", current_date())
        .withColumn("offer_type_id", col(Promotions.des_offer_type.name))
        .withColumn("offer_type", col(Promotions.des_offer_type.name))
        .withColumn("unit", lit("").cast(StringType()))
        .withColumn("restriction", col(Promotions.des_restrictions.name))
        .withColumn("restriction_type", col(Promotions.des_restrictions.name))
        .withColumn("budget", lit("").cast(StringType()))
        .withColumn("offer_startdate", col(Promotions.dat_period_start.name))
        .withColumn("offer_enddate", col(Promotions.dat_period_end.name))
        .withColumn("locations", col(Promotions.des_online_ofline.name))
        .withColumn(
            "offer_name",
            format_string("%s (%s)", col(Promotions.nam_offer.name), col(Promotions.idi_offer.name))
        )
        .withColumn("business_unit", col(Promotions.cde_bu.name))
        .withColumn("description", col(Promotions.des_offer.name))
        .withColumn("image_link", col(Promotions.des_url_image.name))
        .withColumn("image_tag", lit("").cast(StringType()))
        .withColumn("value", col(Promotions.des_offer_value.name))
        .withColumn("draft", lit(0).cast(IntegerType()))
        .withColumn("user_id_id", lit(1).cast(IntegerType()))
        .withColumn("conditional_products_json", lit(empty_valid_json).cast(StringType()))
        .withColumn("incentive_products_json", lit(empty_valid_json).cast(StringType()))
    )

    return mapped_offers.drop(*promo.columns)


promo_columns_mandatory_for_share = [
    Promotions.idi_offer,
    Promotions.nam_offer,
    Promotions.ind_member_visible,
    Promotions.des_offer,
    Promotions.des_url_image,
    Promotions.des_url_mobile_image,
    Promotions.dat_period_start,
    # Promotions.dat_period_end,
    Promotions.des_offer_section,
    Promotions.des_offer_type,
    Promotions.des_terms,
    Promotions.cde_sponsor_key,
    Promotions.nam_sponsor,
    Promotions.des_url_logo,
    Promotions.ind_is_template_based
]

if __name__ == '__main__':
    from awsglue import DynamicFrame
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.transforms import *
    from awsglue.utils import getResolvedOptions

    args = getResolvedOptions(sys.argv, [
        'JOB_NAME', 'lake_descriptor', 'cvm_environment',
        'rds_cvm_connection_name', 'cvm_rds_db_name'
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

    # promotions = create_delta_table_from_catalog(spark, Promotions, lake_descriptor).toDF()
    # promotions = DeltaTable.forPath(spark, "s3://cvm-uat-conformed-d5b175d/sit/promotions/").toDF()
    promotions = spark.read.parquet('s3://cvm-prod-landing-5d6c06b/promotions/share/2023-05-15/')

    # promotions = promotions.select(*[c.name for c in promo_columns_mandatory_for_share])

    promotions = promotions.filter(
        col(Promotions.dat_period_end.name).isNull() | (col(Promotions.dat_period_end.name) > current_date())
    )

    for c in promo_columns_mandatory_for_share:
        promotions = promotions.filter(col(c.name).isNotNull())

    offers = map_promotions_to_offers(promotions)
    print(offers.printSchema())

    offers = DynamicFrame.fromDF(offers, glueContext, 'offers_import')
    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=offers,
        catalog_connection=rds_connection,
        connection_options={
            "database": rds_db_name,  # 'cvm_new',
            "dbtable": 'offerbank_offers',
            "overwrite": "true",
        }
    )
