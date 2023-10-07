from pyspark.sql.functions import *

from cvmdatalake import conformed, nullify_empty_cells, create_delta_table_from_catalog, convert_columns_to_upper_case, \
    features, get_s3_path_for_feature, TableSpec

from cvmdatalake.data_filters import register_data_filter, cvm_data_filter_registry, DataFilterContext


@register_data_filter(
    contex_key=DataFilterContext.clv_share,
    trx_filter_column=conformed.Transactions.bu, filter_value="share",
    mall_filter_column=conformed.Transactions.des_store_name, mall_filter_value="none",
    category="none"
)
@register_data_filter(
    contex_key=DataFilterContext.clv_crf,
    trx_filter_column=conformed.Transactions.bu, filter_value="crf",
    mall_filter_column=conformed.Transactions.des_store_name, mall_filter_value="none",
    category="none"
)
# ten top mall
@register_data_filter(
    contex_key=DataFilterContext.clv_online,
    trx_filter_column=conformed.Transactions.bu, filter_value="share",
    mall_filter_column=conformed.Transactions.des_store_name, mall_filter_value="Online",
    category="none"
)
@register_data_filter(
    contex_key=DataFilterContext.clv_ofemirates,
    trx_filter_column=conformed.Transactions.bu, filter_value="share",
    mall_filter_column=conformed.Transactions.des_store_name, mall_filter_value="Mall of the Emirates",
    category="none"
)
@register_data_filter(
    contex_key=DataFilterContext.clv_citycentremirdif,
    trx_filter_column=conformed.Transactions.bu, filter_value="share",
    mall_filter_column=conformed.Transactions.des_store_name, mall_filter_value="City Centre Mirdif",
    category="none"
)
@register_data_filter(
    contex_key=DataFilterContext.clv_ibnbattuta,
    trx_filter_column=conformed.Transactions.bu, filter_value="share",
    mall_filter_column=conformed.Transactions.des_store_name, mall_filter_value="Ibn Battuta Mall",
    category="none"
)
@register_data_filter(
    contex_key=DataFilterContext.clv_citycentredeira,
    trx_filter_column=conformed.Transactions.bu, filter_value="share",
    mall_filter_column=conformed.Transactions.des_store_name, mall_filter_value="City Centre Deira",
    category="none"
)
@register_data_filter(
    contex_key=DataFilterContext.clv_dubaifestivalcity,
    trx_filter_column=conformed.Transactions.bu, filter_value="share",
    mall_filter_column=conformed.Transactions.des_store_name, mall_filter_value="Dubai Festival City",
    category="none"
)
@register_data_filter(
    contex_key=DataFilterContext.clv_citycentremeaisem,
    trx_filter_column=conformed.Transactions.bu, filter_value="share",
    mall_filter_column=conformed.Transactions.des_store_name, mall_filter_value="City Centre Me'aisem",
    category="none"
)
@register_data_filter(
    contex_key=DataFilterContext.clv_marinamallabudhabi,
    trx_filter_column=conformed.Transactions.bu, filter_value="share",
    mall_filter_column=conformed.Transactions.des_store_name, mall_filter_value="Marina Mall Abu Dhabi",
    category="none"
)
@register_data_filter(
    contex_key=DataFilterContext.clv_carrefouralsaqr,
    trx_filter_column=conformed.Transactions.bu, filter_value="share",
    mall_filter_column=conformed.Transactions.des_store_name, mall_filter_value="Carrefour Al Saqr",
    category="none"
)
@register_data_filter(
    contex_key=DataFilterContext.clv_citycentreajman,
    trx_filter_column=conformed.Transactions.bu, filter_value="share",
    mall_filter_column=conformed.Transactions.des_store_name, mall_filter_value="City Centre Ajman",
    category="none"
)
# 7 top brand
@register_data_filter(
    contex_key=DataFilterContext.clv_carrefour,
    trx_filter_column=conformed.Transactions.bu, filter_value="none",
    mall_filter_column=conformed.Transactions.cde_base_sponsor_key, mall_filter_value="2",
    category="none"
)
@register_data_filter(
    contex_key=DataFilterContext.clv_magicplanet,
    trx_filter_column=conformed.Transactions.bu, filter_value="none",
    mall_filter_column=conformed.Transactions.cde_base_sponsor_key, mall_filter_value="3",
    category="none"
)
@register_data_filter(
    contex_key=DataFilterContext.clv_skidubai,
    trx_filter_column=conformed.Transactions.bu, filter_value="none",
    mall_filter_column=conformed.Transactions.cde_base_sponsor_key, mall_filter_value="35",
    category="none"
)
@register_data_filter(
    contex_key=DataFilterContext.clv_voxcinemas,
    trx_filter_column=conformed.Transactions.bu, filter_value="none",
    mall_filter_column=conformed.Transactions.cde_base_sponsor_key, mall_filter_value="4",
    category="none"
)
@register_data_filter(
    contex_key=DataFilterContext.clv_lululemon,
    trx_filter_column=conformed.Transactions.bu, filter_value="none",
    mall_filter_column=conformed.Transactions.cde_base_sponsor_key, mall_filter_value="13",
    category="none"
)
@register_data_filter(
    contex_key=DataFilterContext.clv_allsaints,
    trx_filter_column=conformed.Transactions.bu, filter_value="none",
    mall_filter_column=conformed.Transactions.cde_base_sponsor_key, mall_filter_value="12",
    category="none"
)
@register_data_filter(
    contex_key=DataFilterContext.clv_cratebarrel,
    trx_filter_column=conformed.Transactions.bu, filter_value="none",
    mall_filter_column=conformed.Transactions.cde_base_sponsor_key, mall_filter_value="14",
    category="none"
)
# custom filters
@register_data_filter(
    contex_key=DataFilterContext.clv_lec,
    trx_filter_column=conformed.Transactions.bu, filter_value="none",
    mall_filter_column=conformed.Transactions.cde_base_bu_key, mall_filter_value="289",
    category="none"
)
@register_data_filter(
    contex_key=DataFilterContext.clv_smbu,
    trx_filter_column=conformed.Transactions.bu, filter_value="none",
    mall_filter_column=conformed.Transactions.cde_base_bu_key, mall_filter_value="290",
    category="none"
)
@register_data_filter(
    contex_key=DataFilterContext.clv_lifestyle,
    trx_filter_column=conformed.Transactions.bu, filter_value="none",
    mall_filter_column=conformed.Transactions.cde_base_bu_key, mall_filter_value="291",
    category="none"

)
# category mapping
@register_data_filter(
    contex_key=DataFilterContext.clv_moeluxury,
    trx_filter_column=conformed.Transactions.bu, filter_value="share",
    mall_filter_column=conformed.Transactions.des_mall_name, mall_filter_value="Mall of the Emirates",
    category="luxury"
)
@register_data_filter(
    contex_key=DataFilterContext.clv_moegroceries,
    trx_filter_column=conformed.Transactions.bu, filter_value="share",
    mall_filter_column=conformed.Transactions.des_store_name, mall_filter_value="Mall of the Emirates",
    category="Hypermarket"
)
@register_data_filter(
    contex_key=DataFilterContext.clv_moefb,
    trx_filter_column=conformed.Transactions.bu, filter_value="share",
    mall_filter_column=conformed.Transactions.des_store_name, mall_filter_value="Mall of the Emirates",
    category="F & B"
)
@register_data_filter(
    contex_key=DataFilterContext.clv_moefashion,
    trx_filter_column=conformed.Transactions.bu, filter_value="share",
    mall_filter_column=conformed.Transactions.des_store_name, mall_filter_value="Mall of the Emirates",
    category="Fashion & Accessories"
)
@register_data_filter(
    contex_key=DataFilterContext.clv_ccmiluxury,
    trx_filter_column=conformed.Transactions.bu, filter_value="share",
    mall_filter_column=conformed.Transactions.des_mall_name, mall_filter_value="City Centre Mirdif",
    category="luxury"
)
@register_data_filter(
    contex_key=DataFilterContext.clv_ccmifashio,
    trx_filter_column=conformed.Transactions.bu, filter_value="share",
    mall_filter_column=conformed.Transactions.des_store_name, mall_filter_value="City Centre Mirdif",
    category="Fashion & Accessories"
)
@register_data_filter(
    contex_key=DataFilterContext.clv_ccmifurnitre,
    trx_filter_column=conformed.Transactions.bu, filter_value="share",
    mall_filter_column=conformed.Transactions.des_store_name, mall_filter_value="City Centre Mirdif",
    category="Home Furnishings"
)
@register_data_filter(
    contex_key=DataFilterContext.clv_ccmibeauty,
    trx_filter_column=conformed.Transactions.bu, filter_value="share",
    mall_filter_column=conformed.Transactions.des_mall_name, mall_filter_value="City Centre Mirdif",
    category="Perfumes & Cosmetics"
)
@register_data_filter(
    contex_key=DataFilterContext.clv_ccmifb,
    trx_filter_column=conformed.Transactions.bu, filter_value="share",
    mall_filter_column=conformed.Transactions.des_store_name, mall_filter_value="City Centre Mirdif",
    category="F & B"
)
@register_data_filter(
    contex_key=DataFilterContext.clv_shareluxury,
    trx_filter_column=conformed.Transactions.bu, filter_value="share",
    mall_filter_column=conformed.Transactions.des_store_name, mall_filter_value="none",
    category="luxury"
)



def filter_for_bu_context(
        trx: DataFrame, prod: DataFrame, trx_filter_column: TableSpec, filter_value: str, mall_filter_column: TableSpec, mall_filter_value: str, category: str
) -> Tuple[DataFrame, DataFrame]:

    if filter_value != "none":
        print(f"Filtering for {trx_filter_column}  {filter_value}")
        trx = trx.filter(col(trx_filter_column.name) == filter_value)

    if mall_filter_value != "none":
        print(f"Filtering for {mall_filter_column}  {mall_filter_value}")
        trx = trx.filter(col(mall_filter_column.name) == mall_filter_value)

    if mall_filter_value == "290":
        trx = nullify_empty_cells(trx)
        trx = trx.filter(col(conformed.Transactions.cde_layered_bu_key.name).isNull())

    if category != "none" and category != "luxury":
        print(f"Filtering for {category}")
        trx = trx.filter(col(conformed.Transactions.des_category_group.name) == category)

    if category == "luxury":
        print(f"Filtering for {category}")
        trx = trx.filter(col(conformed.Transactions.des_store_experience.name) == 'luxury shopping')

    return (
        trx,
        prod
    )


def filter_for_multiple_bus(trx: DataFrame, prod: DataFrame, bu_filters: list[str]) -> Tuple[DataFrame, DataFrame]:
    return (
        trx.filter(col(conformed.Transactions.bu.name).isin(bu_filters)),
        prod.filter(col(conformed.Products.bu.name).isin(bu_filters))
    )


def clv_training(trx: DataFrame, prod: DataFrame) -> DataFrame:
    columns_for_clv_input = [
        conformed.Transactions.idi_counterparty_gr.name,
        conformed.Transactions.idi_proposition.name,
        conformed.Transactions.dat_date_type_1.name,
        conformed.Transactions.cua_amount_type_1.name,
        conformed.Transactions.bu.name,
    ]

    trx = trx.select(*columns_for_clv_input)
    trx = nullify_empty_cells(trx)
    trx = (
        trx
        .dropDuplicates(columns_for_clv_input)
        # .filter(col(conformed.Transactions.dat_date_type_1.name) >= '2021-03-01')
        # .filter(col(conformed.Transactions.idi_counterparty.name).isNotNull())
        .filter(col(conformed.Transactions.idi_counterparty_gr.name).isNotNull())
        .filter(col(conformed.Transactions.dat_date_type_1.name).isNotNull())
        # .filter(col(conformed.Transactions.idi_proposition.name).isNotNull())
        .filter(col(conformed.Transactions.cua_amount_type_1.name).isNotNull())
        .cache()
    )

    columns_for_clv_prod = [
        conformed.Products.idi_proposition.name,
        conformed.Products.des_proposition_status.name
    ]

    prod = prod.select(*columns_for_clv_prod)
    prod = nullify_empty_cells(prod)
    prod = (prod.filter(col(conformed.Products.des_proposition_status.name) == "Active")
            .filter(col(conformed.Products.idi_proposition.name).isNotNull())
            .dropDuplicates(columns_for_clv_prod)
            )

    joined_trx = (
        trx.alias(conformed.Transactions.table_alias())
        .join(
            how='left', other=prod.alias(conformed.Products.table_alias()),
            on=col(
                conformed.Transactions.idi_proposition.column_alias()) == col(
                conformed.Products.idi_proposition.column_alias()
            ))
        .drop(col(conformed.Products.idi_proposition.column_alias()))
        .drop(col(conformed.Products.des_proposition_status.column_alias()))
    )

    joined_trx_agg = (
        joined_trx.groupby(col(conformed.Transactions.idi_counterparty_gr.name),
                           col(conformed.Transactions.dat_date_type_1.name)).agg(
            sum(col(conformed.Transactions.cua_amount_type_1.name)).alias("CUA_AMOUNT_TYPE_1"))

    )

    joined_trx_agg = (
        joined_trx_agg
        .drop(conformed.Transactions.bu.name)
        .withColumn(conformed.Transactions.idi_counterparty.name, col(conformed.Transactions.idi_counterparty_gr.name))
        .drop(conformed.Transactions.idi_counterparty_gr.name)
    )

    joined_trx_agg = joined_trx_agg.select("IDI_COUNTERPARTY", "DAT_DATE_TYPE_1", "CUA_AMOUNT_TYPE_1")

    return convert_columns_to_upper_case(joined_trx_agg)


if __name__ == '__main__':
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions

    args = getResolvedOptions(
        sys.argv,
        ['JOB_NAME', 'lake_descriptor', 'cvm_environment', 'data_filter_context']
    )

    sc = SparkContext()
    glue_context = GlueContext(sc)
    job = Job(glue_context)
    job.init(args['JOB_NAME'], args)

    spark = glue_context.spark_session
    lake_descriptor = args['lake_descriptor']
    cvm_environment = args['cvm_environment']
    data_filter_context = args['data_filter_context']

    trx_df = create_delta_table_from_catalog(spark, conformed.Transactions, lake_descriptor).toDF()
    prod_df = create_delta_table_from_catalog(spark, conformed.Products, lake_descriptor).toDF()

    print(trx_df.count())

    clv_data_filter = cvm_data_filter_registry.get_data_filter(data_filter_context)
    trx_df, prod_df = clv_data_filter(trx=trx_df, prod=prod_df)

    print(trx_df.count())

    result = clv_training(trx_df, prod_df)

    path_to_files = get_s3_path_for_feature(features.ClvTrainingInput, lake_descriptor, data_filter_context)
    print(path_to_files)

    result.write.mode("overwrite").option("header", "true").csv(path=path_to_files)
