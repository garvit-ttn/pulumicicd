from enum import Enum

from pyspark.sql.functions import *
from pyspark.sql.types import *

from cvmdatalake import landing, conformed
from cvmdatalake.conformed import CustomerProfile, CustomAttributes
from cvmdatalake.spark_extensions import *
from jobs.extensions.maf.braze_push import audiences





def test_spark2(spark):
    audience_info = spark.read.parquet(
        's3://cvm-uat-landing-4390bef/cvm-frontend-export/cvm/cdp_audience_information/LOAD00000001.parquet')

    audience_info = audience_info.select('campaign_name', 'raw_query')

    audiences = spark.sparkContext.emptyRDD()

    audiences = audiences.union(audience_info)
    print(audiences.printSchema())

    # profiles = DeltaTable.forPath(spark, 's3://cvm-krz-conformed-7a4f071/test-profile/').toDF()
    # print(profiles.show(5))


def test_parsing():
    col = '24_crf_preferred_city'
    res = ''

    if len(col.split('_')) >= 1 and col.split('_')[0].isdigit():
        res = col[col.index("_") + 1:] + f"_{col.split('_')[0]}"
    else:
        res = col

    assert res == 'crf_preferred_city_24'


def test_prep_uat_data(spark):
    def insert_profiles():
        ids = ['EXSTESTTB', 'EXSTESTMA', 'EXSTESTNR', 'EXSTESTSA', 'EXSTESTSA', 'EXSTESTSA', 'EXSTESTMG']

        profiles_path = '/home/sad/test-profiles'

        profiles = spark.read.format('delta').load(profiles_path).filter(
            col(CustomerProfile.idi_counterparty.name) == '100412')

        all_to_insert = None
        for idi in ids:
            to_insert = (
                profiles.withColumn(CustomerProfile.des_age.name, lit(500).cast(LongType()))
                .withColumn(CustomerProfile.idi_counterparty.name, lit(idi))
            )

            to_insert.show(5)
            all_to_insert = to_insert if all_to_insert is None else all_to_insert.union(to_insert)

        all_to_insert.show(10)
        all_to_insert.write.format('delta').mode('append').save(profiles_path)

    insert_profiles()

    def insert_braze():
        braze_path = '/home/sad/test-profiles'

        idis = {
            'EXSTESTTB': 'SVVuQdMLFALMjI8tg1dHnA==',
            'EXSTESTMA': 'Vj4aB62NVuABazrjrBjT0Q==',
            'EXSTESTNR': 'awBKSRfOuO5jCpnWKTrKAA==',
            'EXSTESTSA': 'VfY8/t2/n7ygiJgViefIjw==',
            'EXSTESTAS': '9Uz+BaIawK69e81GF+qGDA==',
            'EXSTESTFE': 'EfNGRx/G5R9ex4aQpuE36A==',
            'EXSTESTMG': 'Q/OzYrClWtgnYZUQskS/aQ==',
        }

        braze_entries = spark.read.format('delta').load(braze_path).filter(
            col(CustomAttributes.idi_counterparty.name) == '7121859')

        all_to_insert = None
        for k, v in idis.items():
            to_insert = (
                braze_entries
                .withColumn(CustomAttributes.idi_counterparty.name, lit(k))
                .withColumn(CustomAttributes.external_id.name, lit(v))
            )

            to_insert.show(5)
            all_to_insert = to_insert if all_to_insert is None else all_to_insert.union(to_insert)

        all_to_insert.show(10)
        all_to_insert.write.format('delta').mode('append').save(braze_path)

    # print(profiles.select(CustomerProfile.idi_counterparty.name).filter(col('bu') == 'gcr').show(5))


def test_prep_uat_data_read(spark):
    profiles = spark.read.format('delta').load('/home/sad/test-profiles').filter(
        col(CustomerProfile.idi_counterparty.name) == 'EXSTESTMG')

    profiles.show(10)


def test_read_local_parquet(spark):
    braze = spark.read.parquet(
        'file:///home/sad/Downloads/007ffa8c-v_ca_dev_node0008-139769158428416-0.parquet')

    # bu = ColumnSpec(
    #     is_partition=True,
    #     data_type='string',
    #     description='Business Unit'
    # )

    rename_expr = []

    for (col, t) in braze.dtypes:

        print(f"""
   
   {col.lower()} = ColumnSpec(
     data_type='{t}',
     description=''
   )
   
   """)

        res = col
        if len(col.split('_')) >= 1 and col.split('_')[0].isdigit():
            res = col[col.index("_") + 1:] + f"_{col.split('_')[0]}"

        rename_expr.append(f"{col.lower()} as {res.lower()}")


def test_audience_check(spark):
    audience_info = spark.read.format('parquet').load('/home/sad/Downloads/audience_info').select('campaign_name',
                                                                                                  'raw_query')
    audience_info.show(1000, truncate=False)


def test_audience_push(spark):
    data = [
        {"external_id": 'test_id_01', "audience": "test_audience_01", "business_unit": "SHR"},
        {"external_id": 'test_id_01', "audience": "test_audience_02", "business_unit": "SHR"},

        {"external_id": 'test_id_02', "audience": "test_audience_03", "business_unit": "SHR"},
        {"external_id": 'test_id_02', "audience": "test_audience_04", "business_unit": "SHR"},

        {"external_id": 'test_id_03', "audience": "test_audience_03", "business_unit": "SHR"},
        {"external_id": 'test_id_03', "audience": "test_audience_04", "business_unit": "SHR"},

        {"external_id": 'test_id_04', "audience": "test_audience_03", "business_unit": "SHR"},
        {"external_id": 'test_id_04', "audience": "test_audience_04", "business_unit": "SHR"},

        {"external_id": 'test_id_04', "audience": "test_audience_05", "business_unit": "SHR"},
        {"external_id": 'test_id_04', "audience": "test_audience_06", "business_unit": "SHR"},

        {"external_id": 'test_id_05', "audience": "test_audience_03", "business_unit": "SHR+ABC"},
        {"external_id": 'test_id_05', "audience": "test_audience_04", "business_unit": "SHR"},
    ]

    audience_info = spark.createDataFrame(data)
    audience_info.show()

    audiences.run(audience_info)


def test_schema(spark):
    def print_types_mismatch(df, table: TableSpec | Type[Enum]):
        for c, t in df.dtypes:

            if c.lower() not in table.__members__:
                pass

            elif table[c.lower()].value.data_type != t:
                print(f"type mismatch {c.lower()} {t} : {table[c.lower()].value.data_type}")

    def print_types_mismatch2(df: DataFrame, table: TableSpec | Type[Enum]):
        for col in table:
            if col.name.upper() not in df.columns:
                print(f'{col.name}')

    input = spark.read.parquet('/home/sad/Downloads/0c29d5eb-v_ca_dev_node0003-140413231765248-0.parquet')
    print_types_mismatch2(input, landing.CustomAttributes)

    # desc = {staging.Demographics.table_id(): '/home/sad/test-demo'}
    # creat_delta_table_if_not_exists(spark, staging.Demographics, json.dumps(desc))
    #
    # input = spark.read.parquet('/home/sad/Downloads/3ef9b37a-v_ca_dev_node0003-140403873408768-0.parquet')
    # input = input.withColumn('bu', lit('test'))
    # input = input.withColumn('ingestion_date', lit('09.02.2023'))
    #
    # stg_demographics = DeltaTable.forPath(spark, '/home/sad/test-demo')
    # stg_demographics.alias('stg_demographics').merge(
    #     input.alias('lnd_demographics'),
    #     f"""
    #     stg_demographics.{staging.Demographics.bu.name} = lnd_demographics.{landing.Demographics.bu.name} AND
    #     stg_demographics.{staging.Demographics.idi_counterparty.name} = lnd_demographics.{landing.Demographics.idi_counterparty.name}
    #     """
    # ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
