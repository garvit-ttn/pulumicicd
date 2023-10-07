from delta.tables import DeltaTable

from jobs.algorithms.clv.reload_clv import merge_profiles


def test_merge(spark):
    target = DeltaTable.forPath(spark, "/home/sad/Downloads/customer_profile_uat")
    source = DeltaTable.forPath(spark, "/home/sad/Downloads/customer_profile_from_prod").toDF()

    # target = DeltaTable.forPath(spark, "/home/sad/Downloads/profiles-uat.parquet")
    merge_profiles(target, source)
