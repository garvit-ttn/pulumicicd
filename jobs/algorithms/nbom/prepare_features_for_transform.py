from pyspark.sql.types import LongType
from pyspark.sql.functions import *

from cvmdatalake import conformed, nullify_empty_cells, create_delta_table_from_catalog, convert_columns_to_upper_case, \
    features, get_s3_path_for_feature, TableSpec

from cvmdatalake.data_filters import register_data_filter, cvm_data_filter_registry, DataFilterContext


@register_data_filter(
    contex_key=DataFilterContext.nbom_bu_share_brand,
    data_age_motnhs=12,
    trx_filter_column=conformed.Transactions.bu, filter_value="share",
    category="cde_base_sponsor_key"
)
@register_data_filter(
    contex_key=DataFilterContext.nbom_bu_share_proposition,
    data_age_motnhs=6,
    trx_filter_column=conformed.Transactions.bu, filter_value="share",
    category="idi_proposition"
)
@register_data_filter(
    contex_key=DataFilterContext.nbom_bu_share_proposition02,
    data_age_motnhs=12,
    trx_filter_column=conformed.Transactions.bu, filter_value="share",
    category="idi_proposition_level02"
)
@register_data_filter(
    contex_key=DataFilterContext.nbom_bu_share_proposition03,
    data_age_motnhs=12,
    trx_filter_column=conformed.Transactions.bu, filter_value="share",
    category="idi_proposition_level03"
)
@register_data_filter(
    contex_key=DataFilterContext.nbom_bu_share_proposition04,
    data_age_motnhs=6,
    trx_filter_column=conformed.Transactions.bu, filter_value="share",
    category="idi_proposition_level04"
)
# malls
@register_data_filter(
    contex_key=DataFilterContext.nbom_bu_share_mall,
    data_age_motnhs=12,
    trx_filter_column=conformed.Transactions.bu, filter_value="share",
    category="des_store_name"
)
# not working and dosen't make sense
@register_data_filter(
    contex_key=DataFilterContext.nbom_bu,
    data_age_motnhs=12,
    trx_filter_column=conformed.Transactions.bu, filter_value="none",  # none -> all bu's
    category="bu"
)
def filter_for_bu_context():
    pass


def nbom_transform(data_filter_context: str) -> DataFrame:
    data_filter_context = data_filter_context.replace("nbom", "nbo")
    path_to_files = get_s3_path_for_feature(features.NboOutput, lake_descriptor, data_filter_context)
    nbo = spark.read.option("header", True).csv(path_to_files)
    nbo = nbo.withColumnRenamed("nbo", "IDI_PROPOSITION")
    nbo = nbo.dropDuplicates()

    return convert_columns_to_upper_case(nbo)


if __name__ == '__main__':
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions

    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'lake_descriptor', 'cvm_environment', 'data_filter_context'])

    sc = SparkContext()
    glue_context = GlueContext(sc)
    job = Job(glue_context)
    job.init(args['JOB_NAME'], args)

    spark = glue_context.spark_session
    lake_descriptor = args['lake_descriptor']
    cvm_environment = args['cvm_environment']
    data_filter_context = args['data_filter_context']

    result = nbom_transform(data_filter_context)

    path_to_files = get_s3_path_for_feature(features.NbomTransformInput, lake_descriptor, data_filter_context)

    result.repartition(int(result.count() / 100000)).write.mode("overwrite").option("header", "true").csv(
        path=path_to_files)
