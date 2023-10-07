from awsglue.context import GlueContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from delta.tables import *
from pyspark.sql.functions import *

from cvmdatalake import creat_delta_table_if_not_exists, staging, conformed, get_s3_path

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'lake_descriptor', 'cvm_environment'])

sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
spark = glueContext.spark_session

lake_descriptor = args['lake_descriptor']
environment = args['cvm_environment']

stg_trx_df = get_s3_path(staging.Transactions, lake_descriptor)
stg_Transactions_df = DeltaTable.forPath(spark, stg_trx_df).toDF()

creat_delta_table_if_not_exists(spark, conformed.Transactions, lake_descriptor)
conformed_trx_path = get_s3_path(conformed.Transactions, lake_descriptor)
conformed_Transactions = DeltaTable.forPath(spark, conformed_trx_path)

stg_Transactions_df: DataFrame = stg_Transactions_df.dropDuplicates(
    [
        f"{staging.Transactions.idi_turnover.name}",
        f"{staging.Transactions.bu.name}"
    ]
)

conformed_Transactions.alias(conformed.Transactions.table_alias()).merge(
    stg_Transactions_df.alias(staging.Transactions.table_alias()),
    f"""
    {staging.Transactions.idi_turnover.column_alias()} = {conformed.Transactions.idi_turnover.column_alias()} AND
    {staging.Transactions.bu.column_alias()} = {conformed.Transactions.bu.column_alias()} AND
    {staging.Transactions.dat_date_type_1.column_alias()} = {conformed.Transactions.dat_date_type_1.column_alias()}
    """
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
