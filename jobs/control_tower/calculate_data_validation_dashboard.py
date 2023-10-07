from awsglue import DynamicFrame
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from delta.tables import DeltaTable
from pyspark.sql.functions import *
from pyspark.sql.types import StringType

from cvmdatalake import landing, conformed, create_delta_table_from_catalog, create_dynamic_frame_from_catalog,get_s3_path

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'lake_descriptor', 'cvm_environment', 'rds_cvm_connection_name'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


lake_descriptor = args['lake_descriptor']
environment = args['cvm_environment']


lnd_transactionsDynamic: DataFrame = glueContext.create_dynamic_frame.from_catalog(
    database=landing.Transactions.catalog_db_name(environment),
    table_name=landing.Transactions.catalog_table_name(environment),
    transformation_ctx=landing.Transactions.table_id()
).toDF()


# Ingest only the latest offers
latest_ingestion_date = str(lnd_transactionsDynamic.select(max(col(landing.Transactions.ingestion_date.name))).first()[0])
print(f"Ingesting promotions for {latest_ingestion_date}")

lnd_promotions = lnd_transactionsDynamic.filter(col(landing.Transactions.ingestion_date.name) == latest_ingestion_date)
print(f"There are {lnd_promotions.count()} promotions to ingest")


create_delta_table_from_catalog(spark, conformed.DataValidationDashboard, lake_descriptor)
dashboard_path = get_s3_path(conformed.DataValidationDashboard, lake_descriptor)
dashboard = DeltaTable.forPath(spark, dashboard_path)

insertDF = spark.sql("""
INSERT INTO delta.`s3://cvm-prod-conformed-dd6241f/data_validation_dashboard/` ( 
dataset,
data_track_release,
load_date,
load_type,
bu,
no_of_records_as_of_load_date,
no_of_records_as_of_current_date,
primary_key,
primary_key_count_landing,
primary_key_count_staging,
primary_key_count_conformed,
total_count_ids_from_ttn_landing,
total_count_ids_from_staging,
totalhcount_ids_from_conformed,
added_ids_to_conformed,
total_amount_final,
total_amount_added,
athena_dataset_names,
dataset_names_received_from_ttn ) VALUES
('none', 'none','none', 'none', 'none', 'none','none', 'none', null, 'none', 'none', 'none','none','null' ,'none','none','none','none','none')
""")

print('test_insertDF')
print(insertDF.head().num_affected_rows)
print('end_test_insertDF')