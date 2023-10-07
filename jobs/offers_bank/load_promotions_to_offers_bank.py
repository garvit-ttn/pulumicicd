import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, IntegerType

from cvmdatalake import create_delta_table_from_catalog
from cvmdatalake.conformed import Promotions
from cvmdatalake.transformations.promotions import map_promotions_to_details, enrich_with_expiry_column, \
    filter_promotions_for_eligibility

empty_valid_json = json.dumps({})


def map_promotions_to_dynamodb(promo: DataFrame) -> DataFrame:
    offer_columns = [c.name for c in Promotions]
    static_offers = (
        promo
        .withColumn('details', to_json(struct(*offer_columns)))
        .select(Promotions.idi_offer.name, 'details')
    )
    # print("Default Eligible offers for audience after mapping to promotions:"(static_offers, truncate=False))
    return static_offers


def map_promotions_to_offers(promo: DataFrame) -> DataFrame:
    mapped_offers = (
        promo.filter(
            ((lower(Promotions.des_restrictions.name) == 'all') | (
                    lower(Promotions.des_restrictions.name) == 'segment')) &
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
        .withColumn("active", when((col(Promotions.dat_period_start.name) <= current_date()) & (
                col(Promotions.dat_period_end.name) >= current_date()), lit(1).cast(IntegerType())).when(
            (col(Promotions.dat_period_end.name).isNull()), lit(1).cast(IntegerType())))
        .withColumn("brand", col(Promotions.des_brand.name))
        .withColumn("user_id_id", lit(1).cast(IntegerType()))
        .withColumn("conditional_products_json", lit(empty_valid_json).cast(StringType()))
        .withColumn("incentive_products_json", lit(empty_valid_json).cast(StringType()))
        .withColumn("sponsor_name", col(Promotions.nam_sponsor.name))
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

    # db_password_secret_name = args['cvm_offers_db_password_secret_name']
    # db_password = get_secret(db_password_secret_name)

    promotions = create_delta_table_from_catalog(spark, Promotions, lake_descriptor).toDF()
    # promotions = promotions.withColumn("qty_min", lit(" ")).withColumn("qty_max", lit(" ")).withColumn(
    # "ind_can_have_other_discount", lit(" ")).withColumn("ind_apply_after_tax", lit(" ")).withColumn(
    # "ind_can_apply_to_tab", lit(" ")).withColumn("ind_applies_when_purchasing_products", lit(" ")).withColumn(
    # "ind_applies_to_products", lit(" "))
    # promotions = DeltaTable.forPath(spark, "s3://cvm-uat-conformed-d5b175d/sit/promotions/").toDF()
    # promotions = spark.read.parquet('s3://cvm-prod-landing-5d6c06b/promotions/share/2023-05-15/')

    # promotions = promotions.select(*[c.name for c in promo_columns_mandatory_for_share])

    promotions_filtered = promotions.filter(
        ((lower(Promotions.des_restrictions.name) == 'all') | (lower(Promotions.des_restrictions.name) == 'segment')) &
        ((lower(Promotions.des_status.name) == 'launched') | (lower(Promotions.des_status.name) == 'update')) &
        col(Promotions.dat_period_end.name).isNull() | (col(Promotions.dat_period_end.name) > current_date())
    )

    for c in promo_columns_mandatory_for_share:
        promotions_filtered = promotions_filtered.filter(col(c.name).isNotNull())

    offers = map_promotions_to_offers(promotions_filtered)
    print(offers.printSchema())
    print("Offers send to UI table : ", offers.count())
    print("Offers send to UI table", offers.show(truncate=False))

    eligible_offers = map_promotions_to_dynamodb(promotions_filtered)

    offers = DynamicFrame.fromDF(offers, glueContext, 'offers_import')
    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=offers,
        catalog_connection=rds_connection,
        connection_options={
            "database": rds_db_name,  # 'cvm_new',
            "dbtable": 'offerbank_offers_syncing_pipeline_offers',
            "overwrite": "true",
        }
    )

    eligible_offers_for_dynamodb = map_promotions_to_details(
        enrich_with_expiry_column(
            filter_promotions_for_eligibility(promotions)
        ),
        additional_column='expires_at'
    )

    glueContext.write_dynamic_frame.from_options(
        frame=DynamicFrame.fromDF(eligible_offers_for_dynamodb, glueContext, "eligible_offers_for_dynamodb"),
        connection_type="dynamodb",
        connection_options={
            "tableName": "cvm-default-eligible-offers",
            "overwrite": "true"
        }
    )
