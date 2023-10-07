from enum import Enum, auto
from functools import partial
from typing import Callable, Tuple, Any

from pyspark.pandas import DataFrame

DataFilter = Callable[..., Tuple[DataFrame, ...]]


class DataFilterContext(str, Enum):

    @staticmethod
    def _generate_next_value_(name: str, start: int, count: int, last_values: list) -> str:
        return name.replace('_', '.')

    default = "default.all"

    clv_share = auto()
    clv_crf = auto()
    clv_online = auto()
    clv_ofemirates = auto()
    clv_citycentremirdif = auto()
    clv_ibnbattuta = auto()
    clv_citycentredeira = auto()
    clv_dubaifestivalcity = auto()
    clv_citycentremeaisem = auto()
    clv_marinamallabudhabi = auto()
    clv_carrefouralsaqr = auto()
    clv_citycentreajman = auto()

    clv_carrefour = auto()
    clv_magicplanet = auto()
    clv_skidubai = auto()
    clv_voxcinemas = auto()
    clv_lululemon = auto()
    clv_allsaints = auto()
    clv_cratebarrel = auto()
    clv_lec = auto()
    clv_smbu = auto()
    clv_lifestyle = auto()

    clv_shareluxury = auto()
    clv_moeluxury = auto()
    clv_moegroceries = auto()
    clv_moefb = auto()
    clv_moefashion = auto()
    clv_ccmiluxury = auto()
    clv_ccmifashio = auto()
    clv_ccmifurnitre = auto()
    clv_ccmibeauty = auto()
    clv_ccmifb = auto()

    mba_bu_crf = auto()
    mba_bu_lifestyle = auto()
    mba_bu_share_brand = auto()
    mba_bu_share_proposition = auto()
    mba_bu_share_proposition02 = auto()
    mba_bu_share_proposition03 = auto()
    mba_bu_share_proposition04 = auto()

    sat_bu_share_brand = auto()
    sat_bu_share_proposition = auto()
    sat_bu_share_proposition03 = auto()
    sat_bu_share_proposition04 = auto()
    sat_bu_crf_proposition = auto()

    rfm_lec = auto()
    rfm_smbu = auto()
    rfm_lifestyle = auto()
    rfm_sharebrand = auto()
    rfm_shareproposition1 = auto()
    rfm_crfproposition1 = auto()
    rfm_shareproposition3 = auto()
    rfm_carrefour = auto()
    rfm_cnb = auto()
    rfm_that = auto()
    rfm_lll = auto()
    rfm_lego = auto()
    rfm_als = auto()
    rfm_vox = auto()
    rfm_skidubai = auto()
    rfm_magicplanet = auto()
    rfm_ccd = auto()
    rfm_ccmi = auto()
    rfm_moe = auto()

    nbo_bu = auto()
    nbo_sharebrand = auto()
    nbo_sharemall = auto()
    nbo_shareproposition1 = auto()
    nbo_crfproposition1 = auto()
    nbo_shareproposition2 = auto()
    nbo_shareproposition3 = auto()
    nbo_shareproposition4 = auto()
    nbo_shareproposition5 = auto()
    nbo_sharepromotion = auto()

    nbo_moe = auto()
    nbo_ccmi = auto()
    nbo_that = auto()
    nbo_cnb = auto()
    nbo_lll = auto()
    nbo_lego = auto()
    nbo_als = auto()
    nbo_cb2 = auto()
    nbo_anf = auto()
    nbo_hollister = auto()
    nbo_shiseidoo = auto()
    nbo_pf = auto()
    nbo_lifestyle = auto()
    nbo_sharepartners = auto()

    nbom_bu = auto()
    nbom_bu_share_brand = auto()
    nbom_bu_share_mall = auto()
    nbom_bu_share_proposition = auto()
    nbom_bu_share_proposition_six_months = auto()
    nbom_bu_crf_proposition = auto()
    nbom_bu_share_proposition02 = auto()
    nbom_bu_share_proposition03 = auto()
    nbom_bu_share_proposition04 = auto()


class DataFilterRegistry:
    _filters_registry: dict[str, DataFilter] = {}
    _params_registry: dict[str, dict[str, Any]] = {}

    def __init__(self):
        def identity_func(*args, **kwargs):
            kw_values = [kwv for kwn, kwv in kwargs.items()]
            args_values = [a for a in args]
            return tuple(args_values + kw_values)

        self.register_filter(DataFilterContext.default, identity_func)

    def register_filter(self, context_key: str, data_filter: DataFilter, **kwargs: Any):
        self._filters_registry[context_key] = data_filter
        self._params_registry[context_key] = kwargs

    def get_data_filter(self, context_key: str) -> DataFilter:
        data_filter = self._filters_registry[context_key]
        data_filter_params = self._params_registry[context_key]

        return partial(data_filter, **data_filter_params)

    def get_params(self, context_key: str) -> dict[str, Any]:
        return self._params_registry[context_key]


def register_data_filter(contex_key: str, **data_filter_params):
    def register_internal(fun):
        cvm_data_filter_registry.register_filter(contex_key, fun, **data_filter_params)
        return fun

    return register_internal


# module level singleton
cvm_data_filter_registry = DataFilterRegistry()


class Algorithm(str, Enum):
    clv = "clv"
    rfm = "rfm"
    sat = "sat"
    nbo = "nbo"
    nbom = "nbom"
    mba = "mba"


hyper_parameters = {

    f"default.{Algorithm.clv}": {
        "penalizer_coef": "0.01",
        "months_to_predict": "6",
        "testing_period": "9",
        "max_correlation_coef": "0.7",
        "minimum_purchases": "1",
        "training_period": "11",
        "discount_rate": "0.01"
    },

    f"default.{Algorithm.rfm}": {
        "penalizer_coef": "0.01",
        "months_to_predict": "6",
        "testing_period": "9",
        "max_correlation_coef": "0.7",
        "minimum_purchases": "1",
        "training_period": "11",
        "discount_rate": "0.01"
    },

    f"default.{Algorithm.mba}": {
        "minimum_support": "0.001",
        "minimum_confidence": "0.6",
        "minimum_lift": "1",
        "maximum_length": "4"
    },

    f"default.{Algorithm.nbo}": {
        "epochs": "1",
        "embedding_dimensions": "32",
        "number_of_offers": "5",
        "learning_rate": "0.1"
    },

    f"default.{Algorithm.nbom}": {
        "epochs": "1",
        "embedding_dimensions": "32",
        "number_of_offers": "5",
        "learning_rate": "0.1"
    },

    f"default.{Algorithm.sat}": {
        "penalizer_coef": "0.01",
        "months_to_predict": "6",
        "testing_period": "9",
        "max_correlation_coef": "0.7",
        "minimum_purchases": "1",
        "training_period": "11",
        "discount_rate": "0.01"
    },

    DataFilterContext.clv_share: {
        "penalizer_coef": "0.01",
        "months_to_predict": "6",
        "testing_period": "9",
        "max_correlation_coef": "0.7",
        "minimum_purchases": "1",
        "training_period": "11",
        "discount_rate": "0.01"
    },

    DataFilterContext.clv_crf: {
        "penalizer_coef": "0.01",
        "months_to_predict": "6",
        "testing_period": "9",
        "max_correlation_coef": "0.7",
        "minimum_purchases": "1",
        "training_period": "11",
        "discount_rate": "0.01"
    },

}
