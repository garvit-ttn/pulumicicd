import pytest
from pyspark.errors import AnalysisException
from pyspark.sql.functions import lit
from pyspark.sql.types import DateType

from cvmdatalake import conformed, TableSpec
from cvmdatalake.synthetic import fake_df
from cvmdatalake.transformations.promotions import enrich_with_expiry_column

expiry_column = "expires_at"


def test_enrich_with_ttl_column_for_dynamodb(spark):
    promo = fake_df(spark, conformed.Promotions, n=1)
    promo.show()

    enriched = enrich_with_expiry_column(promo)
    enriched.show()

    assert enriched.collect()[0][expiry_column] is not None


class TestPromotion(TableSpec):
    dat_period_end = conformed.Promotions.dat_period_end


def test_empty_end_date_column(spark):
    promo = fake_df(spark, TestPromotion, n=1)
    promo = promo.withColumn(
        conformed.Promotions.dat_period_end.name,
        lit('').cast(DateType())
    )

    promo.show()

    enriched = enrich_with_expiry_column(promo)
    enriched.show()


def test_missing_end_date_column(spark):
    promo = fake_df(spark, TestPromotion, n=1)
    promo = promo.drop(conformed.Promotions.dat_period_end.name)
    promo.show()

    with pytest.raises(
            AnalysisException,
            match="A column or function parameter with name `dat_period_end` cannot be resolved"
    ):
        enrich_with_expiry_column(promo)
