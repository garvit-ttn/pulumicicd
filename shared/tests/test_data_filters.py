from typing import Tuple

from pyspark.sql import DataFrame

from cvmdatalake.data_filters import DataFilterRegistry, register_data_filter, cvm_data_filter_registry, \
    DataFilterContext


@register_data_filter(contex_key="test.contex.braze", bu_filter="braze")
@register_data_filter(contex_key="test.contex.share", bu_filter="share")
def filter_for_bu_context_sample(trx: DataFrame, prod: DataFrame, bu_filter: str) -> Tuple[DataFrame, DataFrame]:
    print(bu_filter)
    return trx + "a", prod + "a"


def test_filter():
    registry = DataFilterRegistry()

    registry.register_filter("clv.bu.share", filter_for_bu_context_sample, bu_filter="share")
    registry.register_filter("clv.bu.crf", filter_for_bu_context_sample, bu_filter="crf")

    bu_share_data_filter = registry.get_data_filter("clv.bu.share")
    share_trx, share_prod = bu_share_data_filter(trx="t1", prod="p1")

    assert share_trx == "t1a" and share_prod == "p1a"

    bu_crf_data_filter = registry.get_data_filter("clv.bu.crf")
    crf_trx, crf_prod = bu_crf_data_filter(trx="t2", prod="p2")

    assert crf_trx == "t2a" and crf_prod == "p2a"


def test_data_filter_composition():
    registry = DataFilterRegistry()

    registry.register_filter("clv.bu.share", filter_for_bu_context_sample, bu_filter="share")
    registry.register_filter("clv.bu.crf", filter_for_bu_context_sample, bu_filter="crf")

    bu_share_data_filter = registry.get_data_filter("clv.bu.share")
    bu_crf_data_filter = registry.get_data_filter("clv.bu.crf")

    trx, prod = bu_share_data_filter(*bu_crf_data_filter(trx="t2", prod="p2"))

    assert trx == "t2aa" and prod == "p2aa"


def test_decorator_registration():
    data_filter = cvm_data_filter_registry.get_data_filter("test.contex.share")
    share_trx, share_prod = data_filter(trx="t1", prod="p1")
    assert share_trx == "t1a" and share_prod == "p1a"

    data_filter = cvm_data_filter_registry.get_data_filter("test.contex.braze")
    braze_trx, braze_prod = data_filter(trx="t1", prod="p1")
    assert braze_trx == "t1a" and braze_prod == "p1a"


def test_default_context():
    data_filter = cvm_data_filter_registry.get_data_filter(DataFilterContext.default)
    share_trx, share_prod = data_filter(trx="t1", prod="p1")
    assert share_trx == "t1" and share_prod == "p1"
