from pyspark.sql.types import LongType
from pyspark.sql.functions import *

from cvmdatalake import conformed, nullify_empty_cells, create_delta_table_from_catalog, convert_columns_to_upper_case, \
    features, get_s3_path_for_feature, TableSpec, ColumnSpec

from cvmdatalake.data_filters import register_data_filter, cvm_data_filter_registry, DataFilterContext


@register_data_filter(
    contex_key=DataFilterContext.nbo_sharebrand,
    data_age_motnhs=6,
    specific_brand="none",
    trx_filter_column=conformed.Transactions.bu, filter_value="share",
    category="cde_base_sponsor_key"
)
@register_data_filter(
    contex_key=DataFilterContext.nbo_shareproposition1,
    data_age_motnhs=6,
    specific_brand="none",
    trx_filter_column=conformed.Transactions.bu, filter_value="share",
    category="idi_proposition"
)
@register_data_filter(
    contex_key=DataFilterContext.nbo_shareproposition2,
    data_age_motnhs=6,
    specific_brand="none",
    trx_filter_column=conformed.Transactions.bu, filter_value="share",
    category="idi_proposition_level03"
)
@register_data_filter(
    contex_key=DataFilterContext.nbo_shareproposition3,
    data_age_motnhs=6,
    specific_brand="none",
    trx_filter_column=conformed.Transactions.bu, filter_value="share",
    category="idi_proposition_level03"
)
@register_data_filter(
    contex_key=DataFilterContext.nbo_shareproposition4,
    data_age_motnhs=4,
    specific_brand="none",
    trx_filter_column=conformed.Transactions.bu, filter_value="share",
    category="idi_proposition_level04"
)
@register_data_filter(
    contex_key=DataFilterContext.nbo_shareproposition5,
    data_age_motnhs=4,
    specific_brand="none",
    trx_filter_column=conformed.Transactions.bu, filter_value="share",
    category="idi_proposition_level05"
)
@register_data_filter(
    contex_key=DataFilterContext.nbo_sharepromotion,
    data_age_motnhs=6,
    specific_brand="none",
    trx_filter_column=conformed.Transactions.bu, filter_value="share",
    category="idi_promotion"
)
# malls
@register_data_filter(
    contex_key=DataFilterContext.nbo_sharemall,
    trx_filter_column=conformed.Transactions.bu, filter_value="share",
    data_age_motnhs=6,
    specific_brand="none",
    category="des_store_name"
)
# not working and dosen't make sense
@register_data_filter(
    contex_key=DataFilterContext.nbo_bu,
    data_age_motnhs=12,
    specific_brand="none",
    trx_filter_column=conformed.Transactions.bu, filter_value="none",  # none -> all bu's
    category="bu"
)
# category mapping
@register_data_filter(
    contex_key=DataFilterContext.nbo_moe,
    trx_filter_column=conformed.Transactions.bu, filter_value="share",
    data_age_motnhs=12,
    specific_brand="Mall of the Emirates",
    category="idi_proposition_level03"
)
@register_data_filter(
    contex_key=DataFilterContext.nbo_ccmi,
    trx_filter_column=conformed.Transactions.bu, filter_value="share",
    data_age_motnhs=12,
    specific_brand="City Centre Mirdif",
    category="idi_proposition_level03"
)
@register_data_filter(
    contex_key=DataFilterContext.nbo_that,
    trx_filter_column=conformed.Transactions.bu, filter_value="share",
    data_age_motnhs=12,
    specific_brand="71",
    category="idi_proposition_level03"
)
@register_data_filter(
    contex_key=DataFilterContext.nbo_cnb,
    trx_filter_column=conformed.Transactions.bu, filter_value="share",
    data_age_motnhs=12,
    specific_brand="14",
    category="idi_proposition_level03"
)
@register_data_filter(
    contex_key=DataFilterContext.nbo_lll,
    trx_filter_column=conformed.Transactions.bu, filter_value="share",
    data_age_motnhs=12,
    specific_brand="13",
    category="idi_proposition_level03"
)
@register_data_filter(
    contex_key=DataFilterContext.nbo_lego,
    trx_filter_column=conformed.Transactions.bu, filter_value="share",
    data_age_motnhs=12,
    specific_brand="38",
    category="idi_proposition_level03"
)
@register_data_filter(
    contex_key=DataFilterContext.nbo_als,
    trx_filter_column=conformed.Transactions.bu, filter_value="share",
    data_age_motnhs=12,
    specific_brand="12",
    category="idi_proposition_level03"
)
@register_data_filter(
    contex_key=DataFilterContext.nbo_cb2,
    trx_filter_column=conformed.Transactions.bu, filter_value="share",
    data_age_motnhs=12,
    specific_brand="77",
    category="idi_proposition_level03"
)
# todo
@register_data_filter(
    contex_key=DataFilterContext.nbo_anf,
    trx_filter_column=conformed.Transactions.bu, filter_value="share",
    data_age_motnhs=12,
    specific_brand="68",
    category="idi_proposition_level03"
)
# todo
@register_data_filter(
    contex_key=DataFilterContext.nbo_hollister,
    trx_filter_column=conformed.Transactions.bu, filter_value="share",
    data_age_motnhs=12,
    specific_brand="83",
    category="idi_proposition_level03"
)
@register_data_filter(
    contex_key=DataFilterContext.nbo_shiseidoo,
    trx_filter_column=conformed.Transactions.bu, filter_value="share",
    data_age_motnhs=12,
    specific_brand="95",
    category="idi_proposition_level03"
)
# todo
@register_data_filter(
    contex_key=DataFilterContext.nbo_pf,
    trx_filter_column=conformed.Transactions.bu, filter_value="share",
    data_age_motnhs=12,
    specific_brand="75",
    category="idi_proposition_level03"
)


def filter_for_bu_context(
        trx: DataFrame, prod: DataFrame, trx_filter_column: ColumnSpec, filter_value: str, category: str, data_age_motnhs: int, specific_brand: str,
) -> Tuple[DataFrame, DataFrame]:
    print(f"Filtering for {trx_filter_column}  {filter_value}")

    prod_cols = [
        conformed.Products.idi_proposition.name,
        conformed.Products.idi_proposition_level02.name,
        conformed.Products.idi_proposition_level03.name,
        conformed.Products.idi_proposition_level04.name,
    ]

    prod = (
        prod.filter(col(conformed.Products.des_proposition_status.name) == 'Active')
        .dropDuplicates([conformed.Products.idi_proposition.name])
        .select(*prod_cols)
    )

    prod = nullify_empty_cells(prod)
    for c in prod_cols:
        prod = prod.filter(col(c).isNotNull())

    if category == "idi_promotion":
        trx = (
            trx.filter(col(conformed.Transactions.idi_promotion.name) != '0')
                .filter(col(conformed.Transactions.idi_promotion.name).isNotNull())
                .filter("idi_promotion not like '%,%'")
        )

    print(f"1 - trx count:{trx.count()}")

    if specific_brand != "none":
        match specific_brand:
            case "Mall of the Emirates":
                trx = trx.filter(col(conformed.Transactions.des_store_name.name) == specific_brand)
            case "City Centre Mirdif":
                trx = trx.filter(col(conformed.Transactions.des_store_name.name) == specific_brand)
            case _:
                trx = trx.filter(col(conformed.Transactions.cde_base_sponsor_key.name) == specific_brand)

    print(f"2 - trx count:{trx.count()}")

    # date filter
    dfdate = trx.select(add_months(max(trx.dat_date_type_1), -data_age_motnhs))
    months_ago = dfdate.collect()[0][0]
    trx = trx.filter(col(conformed.Transactions.dat_date_type_1.name) >= months_ago)


    joined = (
        trx.alias(conformed.Transactions.table_alias())
        .join(
            how='left', other=prod.alias(conformed.Products.table_alias()),
            on=col(
                conformed.Transactions.idi_proposition.column_alias()) == col(
                conformed.Products.idi_proposition.column_alias()))
        .drop(col(conformed.Products.idi_proposition.column_alias()))
    )

    if filter_value != "none":
        joined = joined.filter(col(trx_filter_column.name) == filter_value)

    if category == 'des_store_name':
        joined = joined.filter(col(conformed.Transactions.cde_layered_bu_key.name) == '290')

    return (
        joined.withColumn("CATEGORY", col(category)),
        prod,
    )

# cod_sor
# 971001	Exsell IF
# 971002	Vertica
# 971003	MID
# 971004	Carrefour
# 971005	Share
# 971006	LNE
# 971007	VOX
# 971008	NJM
# 971009	ECP
# 971010	FSN
# 971011	WiFi
# 971012	NI
# 971013	Braze


def nbo_transform_whole_bu(demo: DataFrame, idmapping: DataFrame ) -> DataFrame:
    demographics_columns = [
        conformed.Demographics.idi_counterparty.name
    ]

    print("1_demo")
    print(demo.count())
    print("2_idmapping")
    print(idmapping.count())

    idmapping = idmapping.select(
        conformed.IdMapping.idi_src.name,
        conformed.IdMapping.idi_gcr.name,
        conformed.IdMapping.cod_sor_idi_src.name
    )

    demo = demo.select(*demographics_columns)



    idmapping = idmapping.filter(col(conformed.IdMapping.cod_sor_idi_src.name) == '971005')

    print("3_idmapping")
    print(idmapping.count())

    joined = (
        demo
        .filter(col(conformed.Demographics.bu.name) == 'gcr')
        .alias(conformed.Demographics.table_alias())
        .join(
            how='inner',
            other=idmapping.alias(conformed.IdMapping.table_alias()),
            on=col(conformed.Demographics.idi_counterparty.column_alias()) == col(conformed.IdMapping.idi_gcr.column_alias())
        )
        .dropDuplicates([
            conformed.Demographics.idi_counterparty.name
        ])
    )

    print("4_joined")
    print(joined.count())


    demo_cols = [
        conformed.IdMapping.idi_gcr.name
    ]

    joined = joined.select(*demo_cols).dropDuplicates([conformed.IdMapping.idi_gcr.name])

    joined = (
        joined.withColumn(conformed.Transactions.idi_counterparty.name, col(conformed.IdMapping.idi_gcr.name))
        .select(conformed.Transactions.idi_counterparty.name)
    )

    return convert_columns_to_upper_case(joined)

def nbo_transform_same_as_trainng(trx: DataFrame) -> DataFrame:

    columns_for_input_trx = [
        conformed.Transactions.idi_counterparty_gr.name,
        "CATEGORY"
    ]

    trx = (
        trx.select(*columns_for_input_trx)
        .dropDuplicates([
            conformed.Transactions.idi_counterparty_gr.name,
            "CATEGORY"
        ])
    )

    trx = nullify_empty_cells(trx)
    for c in columns_for_input_trx:
        trx = trx.filter(col(c).isNotNull())

    trx = (
        trx.withColumn(conformed.Transactions.idi_counterparty.name, col(conformed.Transactions.idi_counterparty_gr.name))
        .withColumn(conformed.Transactions.idi_proposition.name, col("CATEGORY"))
        .select(
            conformed.Transactions.idi_counterparty.name,
            conformed.Transactions.idi_proposition.name
        )
    )

    trx = trx.select("idi_counterparty").dropDuplicates()

    return convert_columns_to_upper_case(trx)



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

    demo_df = create_delta_table_from_catalog(spark, conformed.Demographics, lake_descriptor).toDF()
    idmapping_df = create_delta_table_from_catalog(spark, conformed.IdMapping, lake_descriptor).toDF()
    trx_df = create_delta_table_from_catalog(spark, conformed.Transactions, lake_descriptor).toDF()
    prod_df = create_delta_table_from_catalog(spark, conformed.Products, lake_descriptor).toDF()


    mba_data_filter = cvm_data_filter_registry.get_data_filter(data_filter_context)
    trx_df, prod_df = mba_data_filter(trx=trx_df, prod=prod_df)

    if data_filter_context == 'nbo.bu.share.promotion':
        result = nbo_transform_same_as_trainng(trx_df)
    else:
        result = nbo_transform_whole_bu(demo_df, idmapping_df)
    print("Count of rows in result")
    print(result.count())

    path_to_files = get_s3_path_for_feature(features.NboTransformInput, lake_descriptor, data_filter_context)

    result.repartition(int(result.count() / 50000)).write.mode("overwrite").option("header", "false").csv(
        path=path_to_files)
