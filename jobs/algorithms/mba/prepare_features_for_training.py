from pyspark.sql.functions import *
from pyspark.ml.fpm import FPGrowth

from cvmdatalake import conformed, nullify_empty_cells, create_delta_table_from_catalog, convert_columns_to_upper_case, \
    features, get_s3_path_for_feature, TableSpec

from cvmdatalake.data_filters import register_data_filter, cvm_data_filter_registry, DataFilterContext


# @register_data_filter(
#     contex_key=DataFilterContext.clv_bu_rcs,
#     trx_filter_column=conformed.Transactions.bu, filter_value="rcs",
# )
# @register_data_filter(
#     contex_key=DataFilterContext.clv_bu_ni,
#     trx_filter_column=conformed.Transactions.bu, filter_value="ni"
# )
# @register_data_filter(
#     contex_key=DataFilterContext.clv_brand_voxcinemas,
#     trx_filter_column=conformed.Transactions.des_maf_brand_name, filter_value="VOX Cinemas"
# )
# @register_data_filter(
#     contex_key=DataFilterContext.clv_brand_voxoutdoor,
#     trx_filter_column=conformed.Transactions.des_maf_brand_name, filter_value="VOX OUTDOOR"
# )
# @register_data_filter(
#     contex_key=DataFilterContext.mba_bu_share,
#     trx_filter_column=conformed.Transactions.bu, filter_value="share"
# )
@register_data_filter(
    contex_key=DataFilterContext.mba_bu_crf,
    trx_filter_column=conformed.Transactions.bu, filter_value="crf"
)
@register_data_filter(
    contex_key=DataFilterContext.mba_bu_lifestyle,
    trx_filter_column=conformed.Transactions.bu, filter_value="fsn"
)
def filter_for_bu_context(
        trx: DataFrame, prod: DataFrame, trx_filter_column: TableSpec, filter_value: str
) -> Tuple[DataFrame, DataFrame]:
    print(f"Filtering for {trx_filter_column}  {filter_value}")


    # return (
    #     trx.filter(col(trx_filter_column.name) == filter_value),
    #     prod
    # )

    return (
       trx,
        prod
    )


def filter_for_multiple_bus(trx: DataFrame, prod: DataFrame, bu_filters: list[str]) -> Tuple[DataFrame, DataFrame]:
    return (
        trx.filter(col(conformed.Transactions.bu.name).isin(bu_filters)),
        prod.filter(col(conformed.Products.bu.name).isin(bu_filters))
    )

def mba_training_old(trx: DataFrame, prod: DataFrame) -> DataFrame:
    columns_for_mba_input = [
        conformed.Transactions.idi_turnover.name,
        conformed.Transactions.idi_proposition.name,
        conformed.Transactions.idi_counterparty_gr.name,
        conformed.Transactions.idi_promotion.name,
        conformed.Transactions.dat_date_type_1.name,
        conformed.Transactions.cua_amount_type_1.name,
    ]

    trx = (
        trx.filter(col(conformed.Transactions.dat_date_type_1.name) >= '2023-01-15')

    )

    trx = trx.select(*columns_for_mba_input)

    trx_most_1000 = (
        trx.filter(col(conformed.Transactions.idi_proposition.name).isNotNull())
        .groupBy(col(conformed.Transactions.idi_proposition.name)).count().orderBy(col("count").desc()).limit(1000)
        .drop("count")
    )
    trx_most_1000.show()

    trx = nullify_empty_cells(trx)
    trx = (
        trx.dropDuplicates(columns_for_mba_input)
        .filter(col(conformed.Transactions.idi_turnover.name).isNotNull())
        .filter(col(conformed.Transactions.idi_proposition.name).isNotNull())
        .filter(col(conformed.Transactions.idi_counterparty_gr.name).isNotNull())
        .filter(col(conformed.Transactions.dat_date_type_1.name).isNotNull())
        .cache()
    )

    joined_trx = (
        trx.alias("conformed_transactions")
        .join(
            how='inner', other=trx_most_1000.alias("trx_most_1000"),
            on=col("conformed_transactions.idi_proposition") == col("trx_most_1000.idi_proposition"))
        .drop(col("trx_most_1000.idi_proposition"))

    )

    columns_for_mba_prod = [
        conformed.Products.idi_proposition.name,
        conformed.Products.cde_brand.name,
    ]

    prod = prod.select(*columns_for_mba_prod)
    prod = nullify_empty_cells(prod)
    prod = (
        prod
        .filter(col(conformed.Products.des_proposition_status.name) == "Active")
        .filter(col(conformed.Products.idi_proposition.name).isNotNull())
    )

    joined_trx_prod = (
        joined_trx.alias("conformed_transactions")
        .join(
            how='inner', other=prod.alias("prod"),
            on=col("conformed_transactions.idi_proposition") == col("prod.idi_proposition"))
        .drop(col("prod.idi_proposition"))

    )

    joined_trx_prod = joined_trx_prod.withColumn("IDI_TRANSACTION",
                                                 concat(col(conformed.Transactions.idi_counterparty_gr.name).cast(
                                                     StringType()), lit("_"),
                                                        col(conformed.Transactions.dat_date_type_1.name).cast(
                                                            StringType())))
    joined_trx_prod = joined_trx_prod.select("IDI_TRANSACTION", col(conformed.Products.cde_brand.name))

    joined_trx_prod.dropDuplicates()

    # IDI_PROPOSITION

    joined_trx_prod = joined_trx_prod.withColumn("IDI_PROPOSITION", col(conformed.Products.cde_brand.name)).drop(
        col(conformed.Products.cde_brand.name))

    return convert_columns_to_upper_case(joined_trx_prod)


def mba_training(trx: DataFrame, prod: DataFrame) -> DataFrame:
    columns_for_mba_input = [
        conformed.Transactions.idi_counterparty_gr.name,
        conformed.Transactions.cde_base_sponsor_key.name,
        conformed.Transactions.dat_date_type_1.name
    ]

    trx = (
        trx.filter(col(conformed.Transactions.dat_date_type_1.name) >= '2023-01-15')
        .filter(col(conformed.Transactions.cde_base_bu_key.name) == '291')
        .filter(col(conformed.Transactions.bu.name) == 'share')
    )

    trx = trx.select(*columns_for_mba_input)
    trx = nullify_empty_cells(trx)
    trx = (
        trx
        .filter(col(conformed.Transactions.idi_counterparty_gr.name).isNotNull())
        .filter(col(conformed.Transactions.cde_base_sponsor_key.name).isNotNull())
        .dropDuplicates(columns_for_mba_input)
        .cache()
    )
    print("trx")
    trx.show()

    joined_trx = trx.withColumn("IDI_TRANSACTION",
                                concat(col(conformed.Transactions.idi_counterparty_gr.name).cast(StringType()),
                                       lit("_"),
                                       col(conformed.Transactions.dat_date_type_1.name).cast(StringType())))

    joined_trx = joined_trx.withColumn("IDI_TRANSACTION_TEMP",
                                       expr("substring(IDI_TRANSACTION, 1, length(IDI_TRANSACTION)-3)")).drop(
        "IDI_TRANSACTION")

    joined_trx = joined_trx.withColumn("IDI_TRANSACTION", col("IDI_TRANSACTION_TEMP"))

    joined_trx = joined_trx.select("IDI_TRANSACTION", col(conformed.Transactions.cde_base_sponsor_key.name))

    joined_trx = joined_trx.withColumn("IDI_PROPOSITION", col(conformed.Transactions.cde_base_sponsor_key.name)).drop(
        col(conformed.Transactions.cde_base_sponsor_key.name))

    joined_trx = joined_trx.dropDuplicates()

    print("joined_trx")
    joined_trx.show()

    return convert_columns_to_upper_case(joined_trx)

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

    mba_data_filter = cvm_data_filter_registry.get_data_filter(data_filter_context)
    trx_df, prod_df = mba_data_filter(trx=trx_df, prod=prod_df)

    # fp_growth = FPGrowth(itemsCol="items", minSupport=0.5, minConfidence=0.6)

    result = mba_training(trx_df, prod_df)

    path_to_files = get_s3_path_for_feature(features.MbaTrainingInput, lake_descriptor, data_filter_context)
    # path_to_files = "s3://cvm-uat-conformed-d5b175d/features/mba/input/train/default.all.brand.6M/"
    print(path_to_files)

    result.write.mode("overwrite").option("header", "true").parquet(path=path_to_files)
