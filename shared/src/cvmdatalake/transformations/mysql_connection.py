
# audience_info: DataFrame = glueContext.create_dynamic_frame_from_options(
#     connection_type='mysql',
#     connection_options={
#         "database": rds_db_name,
#         "useConnectionProperties": "true",
#         "dbtable": 'cdp_audience_information',
#         "connectionName": rds_connection,
#     }
# ).toDF()