from cvmdatalake import conformed
from cvmdatalake.synthetic import fake_df


def test_fake_df(spark):
    sample = fake_df(spark, conformed.CustomerProfile, n=10)
    sample.show()

    # sample.write.format('delta').mode('overwrite').save("/home/sad/Downloads/sample-profiles-delta-2")
    # sample.write.mode('overwrite').parquet("/home/sad/Downloads/sample-profiles")


def test_fake_df_with_default_fake_columns(spark):
    sample = fake_df(
        spark, conformed.CustomerProfile,
        n=10, n_fake_string_columns=5, n_fake_float_columns=3
    )

    sample.show()

    # sample.write.format('delta').mode('overwrite').save("/home/sad/Downloads/sample-profiles-delta-2")
    # sample.write.mode('overwrite').parquet("/home/sad/Downloads/sample-profiles")
