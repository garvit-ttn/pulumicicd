from delta.tables import DeltaTable

from jobs.offers_bank.load_promotions_to_offers_bank import map_promotions_to_offers


def test_load(spark):
    # promo = spark.read.parquet('/home/sad/Downloads/promotions/bu=share/part-00001-f571ed20-ee93-4169-8acd-dab4cf8907e1.c000.snappy.parquet')

    promo = DeltaTable.forPath(spark, '/home/sad/Downloads/promotions/').toDF()
    print(promo.count())

    offers = map_promotions_to_offers(promo)

    offers.printSchema()
    offers.select("offer_name").show(20, truncate=False)
    offers.count()
