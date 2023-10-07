from awsglue import DynamicFrame
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from delta.tables import DeltaTable
from pyspark.sql.functions import *
from pyspark.sql.types import StringType

from cvmdatalake import landing, conformed, create_delta_table_from_catalog, create_dynamic_frame_from_catalog

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'lake_descriptor', 'cvm_environment', 'rds_cvm_connection_name', 'cvm_rds_db_name'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

rds_connection = args['rds_cvm_connection_name']
lake_descriptor = args['lake_descriptor']
environment = args['cvm_environment']
rds_db_name = args['cvm_rds_db_name']

rds_db_table = 'cmt'

columns_for_cmt_input = [
    landing.Activities.idi_campaign.name,
    landing.Activities.des_campaign_name.name,
    landing.Activities.des_activity_type.name,
    landing.Activities.des_activity_channel.name,
    landing.Activities.des_marketing_area.name,
    landing.Activities.des_message_action.name,
    landing.Activities.idi_counterparty_gr.name
]

activities = create_dynamic_frame_from_catalog(glueContext, table=landing.Activities, prefix=environment).toDF()

cmt = (
    activities
    .filter(year(landing.Activities.dat_campaign_start_date.name) == '2023')
    .filter(month(landing.Activities.dat_campaign_start_date.name) == '04')
    .select(*columns_for_cmt_input)
    .groupby("idi_campaign", "des_campaign_name", "des_activity_type", "des_activity_channel", "des_marketing_area",
             "des_message_action").agg(countDistinct("idi_counterparty_gr").alias("count_idi_counterparty_gr"))

)

cmt.show()

# cast all to string
for c in cmt.columns:
    cmt = cmt.withColumn(c, col(c).cast(StringType()))

print(cmt.printSchema())

cmt_to_import = DynamicFrame.fromDF(cmt, glueContext, 'cmt_to_import')

glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=cmt_to_import,
    catalog_connection=rds_connection,
    connection_options={
        "database": rds_db_name,
        "dbtable": rds_db_table,
        "overwrite": "true",
    }
)
