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

rds_db_table = 'cmt2'

columns_for_cmt_input = [
    landing.Activities.dat_batch.name,
    landing.Activities.idi_activity.name,
    landing.Activities.cod_sor_activity.name,
    landing.Activities.ind_counterparty_type.name,
    landing.Activities.cod_sor_counterparty.name,
    landing.Activities.cod_sor_counterparty_gr.name,
    landing.Activities.des_activity_type.name,
    landing.Activities.des_activity_channel.name,
    landing.Activities.dat_activity.name,
    landing.Activities.tim_activity.name,
    landing.Activities.des_campaign_name.name,
    landing.Activities.idi_user_id.name,
    landing.Activities.des_email_address.name,
    landing.Activities.des_marketing_area.name,
    landing.Activities.des_message_action.name,
    landing.Activities.des_message_event.name,
    landing.Activities.dat_campaign_start_date.name,
    landing.Activities.dat_campaign_end_date.name,
    landing.Activities.des_campaign_category.name,
    landing.Activities.dat_processing_date.name,
    landing.Activities.des_impression.name,
    landing.Activities.idi_ga_session_id.name,
    landing.Activities.ingestion_date.name,
]

activities = create_dynamic_frame_from_catalog(glueContext, table=landing.Activities, prefix=environment).toDF()

cmt = (
    activities
    .orderBy(col(landing.Activities.dat_campaign_start_date.name).desc())
    .limit(1000000)
    .select(*columns_for_cmt_input)
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
