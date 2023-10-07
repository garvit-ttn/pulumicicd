from delta.tables import DeltaTable
from pyspark.sql.functions import lit

from cvmdatalake.conformed import Promotions
from jobs.extensions.maf.apis.load_next_best_offers import get_random_promotions, \
    assign_default_offers, enrich_with_other_ids


def test_happy(spark):
    promo_df = DeltaTable.forPath(spark, "/home/sad/Downloads/promotions").toDF()

    unified_offers = get_random_promotions(spark, promo_df)

    unified_offers.printSchema()
    unified_offers.show(truncate=False)


def test_loading_for_50k_share_user(spark):
    promo_df = spark.read.parquet('/home/sad/Downloads/share-offers')
    promo_df = promo_df.withColumn(Promotions.bu.name, lit('share'))

    customers_df = spark.read.parquet(
        '/home/sad/.config/JetBrains/PyCharm2022.3/scratches/sample50k/share_customers.parquet')

    id_map = DeltaTable.forPath(spark, '/home/sad/Downloads/id_mapping').toDF()

    result = assign_default_offers(customers_df, promo_df, id_map)

    result.printSchema()
    result.show(100, truncate=False)


def test_enrich_customers_with_ids(spark):
    customers_df = spark.read.parquet(
        '/home/sad/.config/JetBrains/PyCharm2022.3/scratches/sample50k/share_customers.parquet')

    id_map = DeltaTable.forPath(spark, '/home/sad/Downloads/id_mapping').toDF()

    result = enrich_with_other_ids(customers_df, id_map)

    result.printSchema()
    result.show(100, truncate=False)
