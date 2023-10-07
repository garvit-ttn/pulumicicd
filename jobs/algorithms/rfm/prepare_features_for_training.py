from pyspark.sql.types import LongType
from pyspark.sql.functions import *

from cvmdatalake import conformed, nullify_empty_cells, create_delta_table_from_catalog, convert_columns_to_upper_case, \
    features, get_s3_path_for_feature, TableSpec

from cvmdatalake.data_filters import register_data_filter, cvm_data_filter_registry, DataFilterContext


@register_data_filter(
    contex_key=DataFilterContext.rfm_sharebrand,
    trx_filter_column=conformed.Transactions.bu, filter_value="share",
    category_filter_column="cde_base_sponsor_key",
    category_filter_columm_present="IDI_PROPOSITION"
)
@register_data_filter(
    contex_key=DataFilterContext.rfm_shareproposition1,
    trx_filter_column=conformed.Transactions.bu, filter_value="share",
    category_filter_column="idi_proposition",
    category_filter_columm_present="IDI_PROPOSITION"
)
@register_data_filter(
    contex_key=DataFilterContext.rfm_shareproposition3,
    trx_filter_column=conformed.Transactions.bu, filter_value="share",
    category_filter_column="idi_proposition_level03",
    category_filter_columm_present="IDI_PROPOSITION"
)
@register_data_filter(
    contex_key=DataFilterContext.rfm_crfproposition1,
    trx_filter_column=conformed.Transactions.bu, filter_value="crf",
    category_filter_column="idi_proposition",
    category_filter_columm_present="IDI_PROPOSITION"
)
# brand
@register_data_filter(
    contex_key=DataFilterContext.rfm_carrefour,
    trx_filter_column=conformed.Transactions.cde_base_sponsor_key, filter_value="2",
    category_filter_column="idi_proposition",
    category_filter_columm_present="IDI_PROPOSITION"
)
@register_data_filter(
    contex_key=DataFilterContext.rfm_cnb,
    trx_filter_column=conformed.Transactions.cde_base_sponsor_key, filter_value="14",
    category_filter_column="idi_proposition",
    category_filter_columm_present="IDI_PROPOSITION"
)
@register_data_filter(
    contex_key=DataFilterContext.rfm_that,
    trx_filter_column=conformed.Transactions.cde_base_sponsor_key, filter_value="71",
    category_filter_column="idi_proposition",
    category_filter_columm_present="IDI_PROPOSITION"
)
@register_data_filter(
    contex_key=DataFilterContext.rfm_lll,
    trx_filter_column=conformed.Transactions.cde_base_sponsor_key, filter_value="13",
    category_filter_column="idi_proposition",
    category_filter_columm_present="IDI_PROPOSITION"
)
@register_data_filter(
    contex_key=DataFilterContext.rfm_lego,
    trx_filter_column=conformed.Transactions.cde_base_sponsor_key, filter_value="38",
    category_filter_column="idi_proposition",
    category_filter_columm_present="IDI_PROPOSITION"
)
@register_data_filter(
    contex_key=DataFilterContext.rfm_als,
    trx_filter_column=conformed.Transactions.cde_base_sponsor_key, filter_value="12",
    category_filter_column="idi_proposition",
    category_filter_columm_present="IDI_PROPOSITION"
)
@register_data_filter(
    contex_key=DataFilterContext.rfm_vox,
    trx_filter_column=conformed.Transactions.cde_base_sponsor_key, filter_value="4",
    category_filter_column="idi_proposition",
    category_filter_columm_present="IDI_PROPOSITION"
)
@register_data_filter(
    contex_key=DataFilterContext.rfm_skidubai,
    trx_filter_column=conformed.Transactions.cde_base_sponsor_key, filter_value="35",
    category_filter_column="idi_proposition",
    category_filter_columm_present="IDI_PROPOSITION"
)
@register_data_filter(
    contex_key=DataFilterContext.rfm_magicplanet,
    trx_filter_column=conformed.Transactions.cde_base_sponsor_key, filter_value="3",
    category_filter_column="idi_proposition",
    category_filter_columm_present="IDI_PROPOSITION"
)
#custom lec smbu
@register_data_filter(
    contex_key=DataFilterContext.rfm_lec,
    trx_filter_column=conformed.Transactions.cde_base_bu_key, filter_value="289",
    category_filter_column="idi_proposition",
    category_filter_columm_present="IDI_PROPOSITION"
)
@register_data_filter(
    contex_key=DataFilterContext.rfm_smbu,
    trx_filter_column=conformed.Transactions.cde_layered_bu_key, filter_value="290",
    category_filter_column="idi_proposition",
    category_filter_columm_present="IDI_PROPOSITION"
)
@register_data_filter(
    contex_key=DataFilterContext.rfm_lifestyle,
    trx_filter_column=conformed.Transactions.cde_base_bu_key, filter_value="291",
    category_filter_column="idi_proposition",
    category_filter_columm_present="IDI_PROPOSITION"
)
# malls
@register_data_filter(
    contex_key=DataFilterContext.rfm_ccd,
    trx_filter_column=conformed.Transactions.des_store_name, filter_value="City Centre Deira",
    category_filter_column="idi_proposition",
    category_filter_columm_present="IDI_PROPOSITION"
)
@register_data_filter(
    contex_key=DataFilterContext.rfm_ccmi,
    trx_filter_column=conformed.Transactions.des_store_name, filter_value="City Centre Mirdif",
    category_filter_column="idi_proposition",
    category_filter_columm_present="IDI_PROPOSITION"
)
@register_data_filter(
    contex_key=DataFilterContext.rfm_moe,
    trx_filter_column=conformed.Transactions.des_store_name, filter_value="Mall of the Emirates",
    category_filter_column="idi_proposition",
    category_filter_columm_present="IDI_PROPOSITION"
)


def filter_for_bu_context(
        trx: DataFrame, prod: DataFrame, trx_filter_column: TableSpec, filter_value: str, category_filter_column: str,
        category_filter_columm_present: str
) -> Tuple[DataFrame, DataFrame, str]:
    print(f"Filtering for {trx_filter_column}  {filter_value}")

    prod_cols = [
        conformed.Products.idi_proposition.name,
        conformed.Products.idi_proposition_level03.name
    ]

    prod = (
        prod.filter(col(conformed.Products.des_proposition_status.name) == 'Active')
        .dropDuplicates([conformed.Products.idi_proposition.name])
        .select(*prod_cols)
    )

    prod = nullify_empty_cells(prod)
    for c in prod_cols:
        prod = prod.filter(col(c).isNotNull())

    joined = (
        trx.alias(conformed.Transactions.table_alias())
        .join(
            how='left', other=prod.alias(conformed.Products.table_alias()),
            on=col(
                conformed.Transactions.idi_proposition.column_alias()) == col(
                conformed.Products.idi_proposition.column_alias()))
        .drop(col(conformed.Products.idi_proposition.column_alias()))
    )

    joined = joined.filter(col(trx_filter_column.name) == filter_value)

    return (
        joined.withColumn("CATEGORY", col(category_filter_column)),
        prod,
        category_filter_columm_present
    )


def filter_for_multiple_bus(trx: DataFrame, prod: DataFrame, bu_filters: list[str]) -> Tuple[DataFrame, DataFrame]:
    return (
        trx.filter(col(conformed.Transactions.bu.name).isin(bu_filters)),
        prod.filter(col(conformed.Products.bu.name).isin(bu_filters))
    )


def rfm_training(trx: DataFrame, prod: DataFrame) -> DataFrame:


    # Transactions
    columns_for_input_trx = [
        conformed.Transactions.idi_counterparty_gr.name,
        conformed.Transactions.dat_date_type_1.name,
        conformed.Transactions.cua_amount_type_1.name,
        "CATEGORY"
    ]

    trx = (
        trx.select(*columns_for_input_trx)
    )

    trx = nullify_empty_cells(trx)
    # for c in columns_for_input_trx:
    #     trx = trx.filter(col(c).isNotNull())

    trx = (
        trx.filter(col(conformed.Transactions.idi_counterparty_gr.name).isNotNull())
        .filter(col(conformed.Transactions.cua_amount_type_1.name).isNotNull())
        # .filter(col(conformed.Transactions.idi_proposition.name).isNotNull())
        .withColumn(conformed.Transactions.idi_counterparty.name, col(conformed.Transactions.idi_counterparty_gr.name))
        .fillna(0, [conformed.Transactions.cua_amount_type_1.name])
        .select(
            conformed.Transactions.idi_counterparty.name,
            conformed.Transactions.dat_date_type_1.name,
            conformed.Transactions.cua_amount_type_1.name,
            "CATEGORY")
    )

    trx = (
        trx.groupby(
            conformed.Transactions.idi_counterparty.name,
            conformed.Transactions.dat_date_type_1.name,
            "CATEGORY"
        )
        .agg(sum(conformed.Transactions.cua_amount_type_1.name).alias(conformed.Transactions.cua_amount_type_1.name))
    )

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

    trx_df = create_delta_table_from_catalog(spark, conformed.Transactions, lake_descriptor).toDF()
    prod_df = create_delta_table_from_catalog(spark, conformed.Products, lake_descriptor).toDF()

    rfm_data_filter = cvm_data_filter_registry.get_data_filter(data_filter_context)
    trx_df, prod_df, category_filter_columm_present = rfm_data_filter(trx=trx_df, prod=prod_df)

    result = rfm_training(trx_df, prod_df)

    print(category_filter_columm_present)
    print(f"Changing result CATEGORY to {category_filter_columm_present}")
    result = result.withColumn(category_filter_columm_present, col("CATEGORY")).drop("CATEGORY")

    if category_filter_columm_present == 'IDI_PROPOSITION':
        result = result.drop("IDI_PROPOSITION")


    print(data_filter_context)



    result.write.mode("overwrite").option("header", "true").csv(
        path=get_s3_path_for_feature(features.RfmTrainingInput, lake_descriptor, data_filter_context)
    )
