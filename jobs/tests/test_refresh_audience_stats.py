from jobs.offers_bank.refresh_audiences_stats import refresh_audience_details


def test_happy_path(spark):
    df1 = spark.createDataFrame([{
        'co1': 'asd',
        'co2': 'asd',
    }])

    df2 = spark.createDataFrame([{
        'co1': 'asd',
        'co2': 'asd',
    }])

    result = refresh_audience_details(df1, df2)

    print(result.printSchema())
    print(result.show())
