from jobs.offers_bank.calculate_cdp_results_stats import compute_cdp_results_stats


def test_happy_path(spark):
    profiles = spark.read.parquet('/home/sad/Downloads/profile.parquet')

    values, stats = compute_cdp_results_stats(spark, profiles)

    values.printSchema()
    values.show(50)

    stats.printSchema()
    stats.show(50)
