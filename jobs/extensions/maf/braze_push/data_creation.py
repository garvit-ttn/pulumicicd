import sys

from pyspark import SparkContext

from cvmdatalake import conformed, create_delta_table_from_catalog, creat_delta_table_if_not_exists, get_s3_path
from cvmdatalake import creat_delta_table_if_not_exists, get_s3_path, features, convert_columns_to_lower_case
from cvmdatalake.conformed import *



if __name__ == '__main__':
    from awsglue import DynamicFrame
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from delta import DeltaTable

    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'lake_descriptor', 'cvm_environment',
                                         'rds_cvm_connection_name', 'cvm_rds_db_name'
                                         ])

    sc = SparkContext()
    glueContext = GlueContext(sc)
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    spark = glueContext.spark_session

    lake_descriptor = args['lake_descriptor']
    environment = args['cvm_environment']
    rds_connection = args['rds_cvm_connection_name']
    rds_db_name = args['cvm_rds_db_name']

    customer_profiles = create_delta_table_from_catalog(spark, conformed.CustomerProfile, lake_descriptor).toDF()
    print(customer_profiles.count())

    # braze_attributes = spark.read.parquet('s3://cvm-prod-landing-5d6c06b/bu_external_id/2023-07-05/')
    # braze_attributes = braze_attributes.filter("org_code" == "CRF").select('gcr_id')
    # braze_attributes.show(truncate=False)


    df1 = spark.createDataFrame([['1000005','971002','AL AIN',35,'CRF LYL VOX WFI'],
                                 ['10000171','971002','AL AIN',34,'CRF LYL VOX WFI'],
                                 ['10000178', '971002', 'AL AIN', 43, 'CRF LYL VOX WFI'],
                                 ['10000189', '971002', 'AL AIN', 34, 'CRF LYL VOX WFI'],
                                 ['10000230', '971002', 'AL AIN', 25, 'CRF LYL VOX WFI'],
                                 ['10000245', '971002', 'AL AIN', 44, 'CRF LYL VOX WFI'],
                                 ['10000264', '971002', 'AL AIN', 45, 'CRF LYL VOX WFI']] ,
                                ['idi_counterparty', 'cod_sor_counterparty', 'adr_visit_cityname',
                                 'des_age', 'counterparty_role' ])
    df1.show()

    final = df1.unionByName(customer_profiles,allowMissingColumns=True)

    final.show(truncate=False)
    print(final.count())

    # fjoin = final.join(braze_attributes,
    #                   customer_profiles.idi_counterparty == braze_attributes.gcr_id
    #                   ,"inner")
    #
    # fjoin.show()

    creat_delta_table_if_not_exists(spark, CustomerProfile, lake_descriptor)
    # profiles_s3_path = get_s3_path(CustomerProfile, lake_descriptor)

    # profiles_s3_path = 's3://cvm-prod-conformed-dd6241f/customer_profile_test_2023_07_06'

    profiles_s3_path = 's3://cvm-uat-conformed-d5b175d/cusotmer_profiles_delta/customer_profile'

    final.write.format('delta').mode('overwrite').save(profiles_s3_path)


