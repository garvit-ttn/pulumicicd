from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from delta import DeltaTable
from pyspark.sql.functions import *
from pyspark.sql.types import StringType

from cvmdatalake import creat_delta_table_if_not_exists, staging, conformed, get_s3_path

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'lake_descriptor', 'cvm_environment'])

sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
spark = glueContext.spark_session

lake_descriptor = args['lake_descriptor']
environment = args['cvm_environment']

conformed_trx_path = get_s3_path(conformed.CustomAttributes, lake_descriptor)

stg_map_path = get_s3_path(staging.CustomAttributes, lake_descriptor)
stg_custom_attribs = DeltaTable.forPath(spark, stg_map_path).toDF()

stg_custom_attribs = (
    stg_custom_attribs.withColumnRenamed("country_of_residence", "cde_country_of_residence")
    .withColumnRenamed("date_of_birth", "dat_birth")
    .withColumnRenamed("enrollment_date", "dat_enrollment")
    .withColumnRenamed("first_login_date", "dat_first_login")
    .withColumnRenamed("last_accrual_date", "dat_last_accrual")
    .withColumnRenamed("last_activity_date", "dat_last_activity")
    .withColumnRenamed("last_login_date", "dat_last_login")
    .withColumnRenamed("last_redemption_date", "dat_last_redemption")
    .withColumnRenamed("language", "des_language")
    .withColumnRenamed("customer_language", "des_language_customer")
    .withColumnRenamed("membership_stage", "des_membership_stage")
    .withColumnRenamed("nationality", "des_nationality")
    .withColumnRenamed("summer_activity", "des_summer_activity")
    .withColumnRenamed("ken_18", "ken_18_plus")
    .withColumnRenamed("brand", "nam_brand")
    .withColumnRenamed("country", "nam_country")
    .withColumnRenamed("enrollment_sponsor", "nam_enrollment_sponsor")
    .withColumnRenamed("title", "nam_title")
    .withColumnRenamed("total_accrued", "qty_accrued_total")
    .withColumnRenamed("points_balance", "qty_points_balance")
    .withColumnRenamed("points_won_spin_wheel", "qty_points_won_spin_wheel")
    .withColumnRenamed("total_redeemed", "qty_redeemed_total")
    .withColumnRenamed("mall_code", "cde_mal")
    .withColumnRenamed("share_registration_date", "dat_registration_share")
    .withColumnRenamed("first_share_login_date", "dat_first_login_share")
)

stg_custom_attribs = (
    stg_custom_attribs.withColumn("ide_telephone", lit(None).cast(StringType()))
)

stg_custom_attribs = stg_custom_attribs.dropDuplicates(["IDI_COUNTERPARTY"])

creat_delta_table_if_not_exists(spark, conformed.CustomAttributes, lake_descriptor)
conformed_custom_attribs_path = get_s3_path(conformed.CustomAttributes, lake_descriptor)

conformed_custom_attribs = DeltaTable.forPath(spark, conformed_custom_attribs_path)
conformed_custom_attribs.alias(conformed.CustomAttributes.table_alias()).merge(
    stg_custom_attribs.alias(staging.CustomAttributes.table_alias()),
    f"""
    {staging.CustomAttributes.idi_counterparty.column_alias()} = {conformed.CustomAttributes.idi_counterparty.column_alias()}
    """
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
