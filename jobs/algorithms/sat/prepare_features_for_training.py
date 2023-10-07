from pyspark.sql.types import LongType
from pyspark.sql.functions import *

from cvmdatalake import conformed, nullify_empty_cells, create_delta_table_from_catalog, convert_columns_to_upper_case, \
    features, get_s3_path_for_feature, TableSpec

from cvmdatalake.data_filters import register_data_filter, cvm_data_filter_registry, DataFilterContext


@register_data_filter(
    contex_key=DataFilterContext.sat_bu_share_brand,
    contex_key_name=DataFilterContext.sat_bu_share_brand.name,
    demo_filter_column=conformed.IdMapping.cod_sor_idi_src, demo_filter_value="971005",
    trx_filter_column=conformed.Transactions.bu, trx_filter_value="share",
    category_filter_column="cde_base_sponsor_key",
    category_filter_columm_present="NAM_BRAND"
)
@register_data_filter(
    contex_key=DataFilterContext.sat_bu_share_proposition,
    contex_key_name=DataFilterContext.sat_bu_share_proposition.name,
    demo_filter_column=conformed.IdMapping.cod_sor_idi_src, demo_filter_value="971005",
    trx_filter_column=conformed.Transactions.bu, trx_filter_value="share",
    category_filter_column="idi_proposition",
    category_filter_columm_present="IDI_PROPOSITION_LEVEL03"
)
@register_data_filter(
    contex_key=DataFilterContext.sat_bu_share_proposition03,
    contex_key_name=DataFilterContext.sat_bu_share_proposition03.name,
    demo_filter_column=conformed.IdMapping.cod_sor_idi_src, demo_filter_value="971005",
    trx_filter_column=conformed.Transactions.bu, trx_filter_value="share",
    category_filter_column="idi_proposition_level03",
    category_filter_columm_present="IDI_PROPOSITION_LEVEL03"
)
@register_data_filter(
    contex_key=DataFilterContext.sat_bu_share_proposition04,
    contex_key_name=DataFilterContext.sat_bu_share_proposition04.name,
    demo_filter_column=conformed.IdMapping.cod_sor_idi_src, demo_filter_value="971005",
    trx_filter_column=conformed.Transactions.bu, trx_filter_value="share",
    category_filter_column="idi_proposition_level04",
    category_filter_columm_present="IDI_PROPOSITION_LEVEL04"
)
@register_data_filter(
    contex_key=DataFilterContext.sat_bu_crf_proposition,
    contex_key_name=DataFilterContext.sat_bu_crf_proposition.name,
    demo_filter_column=conformed.IdMapping.cod_sor_idi_src, demo_filter_value="971004",
    trx_filter_column=conformed.Transactions.bu, trx_filter_value="crf",
    category_filter_column="idi_proposition",
    category_filter_columm_present="IDI_PROPOSITION_LEVEL03"
)
def filter_for_bu_context(trx: DataFrame, prod: DataFrame, demo: DataFrame, pao: DataFrame, idmap: DataFrame,
                          contex_key_name: str, demo_filter_column: TableSpec, demo_filter_value: str,
                          trx_filter_column: TableSpec, trx_filter_value: str, category_filter_column: str,
                          category_filter_columm_present: str) \
        -> Tuple[DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, str]:
    print(contex_key_name)

    trx = (
        trx.filter(col(conformed.Transactions.dat_date_type_1.name) >= '2022-11-10')
        .filter(col(conformed.Transactions.bu.name) == 'share')
    )

    demo = demo.filter(col(conformed.Demographics.bu.name) == 'gcr')

    print("demo")
    print(demo.show())
    print(demo.count())

    # profile algorithms output rfm.bu.share.brand5
    # pao = pao.filter(col(conformed.ProfileAlgorithmsOutput.data_filter_context.name) == contex_key_name.replace('sat','rfm').replace('_', '.'))
    #

    pao = pao.filter(col(conformed.ProfileAlgorithmsOutput.data_filter_context.name) == "rfm.bu.share/")  # TODO
    latest_date_of_prepare = pao.select(max(col(conformed.ProfileAlgorithmsOutput.date_of_prepare.name))).collect()[0][0]
    print("RFM - latest_date_of_prepare")
    print(latest_date_of_prepare)
    pao.filter(col(conformed.ProfileAlgorithmsOutput.date_of_prepare.name) == latest_date_of_prepare)

    # trx add category

    prod_cols = [
        conformed.Products.idi_proposition.name,
        conformed.Products.idi_proposition_level03.name,
        conformed.Products.idi_proposition_level04.name
    ]

    prod = (
        prod.filter(col(conformed.Products.des_proposition_status.name) == 'Active')
        .dropDuplicates([conformed.Products.idi_proposition.name])
        .select(*prod_cols)
    )

    prod = nullify_empty_cells(prod)
    for c in prod_cols:
        prod = prod.filter(col(c).isNotNull())

    trx_cat = (
        trx.alias(conformed.Transactions.table_alias())
        .join(
            how='left', other=prod.alias(conformed.Products.table_alias()),
            on=col(
                conformed.Transactions.idi_proposition.column_alias()) == col(
                conformed.Products.idi_proposition.column_alias()))
        .drop(col(conformed.Products.idi_proposition.column_alias()))
    )

    trx_cat = trx_cat.withColumn("CATEGORY", col(category_filter_column))
    trx_cat = trx_cat.filter(col(trx_filter_column.name) == trx_filter_value)

    # trx filter

    trx = trx.filter(col(trx_filter_column.name) == trx_filter_value)

    return (
        trx,
        trx_cat,
        prod,
        demo,
        pao,
        category_filter_columm_present
    )


def sat_training(trx: DataFrame, trx_cat: DataFrame, prod: DataFrame, demo: DataFrame, pao: DataFrame) -> DataFrame:
    columns_for_input_trx = [
        # conformed.Transactions.idi_turnover.name,
        conformed.Transactions.idi_counterparty_gr.name,
        conformed.Transactions.idi_proposition.name,
        conformed.Transactions.dat_date_type_1.name,
        conformed.Transactions.cua_amount_type_1.name,
    ]

    columns_for_input_demo = [
        conformed.Demographics.des_age.name,
        conformed.Demographics.adr_visit_cde_country.name,
        conformed.Demographics.ind_person_gender.name,
        conformed.Demographics.idi_counterparty.name,
    ]

    columns_for_input_pao = [
        features.RfmOutput.idi_counterparty.name,
        features.RfmOutput.customer_segment.name
    ]

    columns_for_input_trx_cat = [
        conformed.Transactions.idi_proposition.name,
        "CATEGORY"
    ]

    trx_cat = trx_cat.select(*columns_for_input_trx_cat)
    trx_cat = trx_cat.dropDuplicates(columns_for_input_trx_cat)

    print("1_trx_cat")
    print(trx_cat.count())
    trx_cat.show()

    trx = trx.select(*columns_for_input_trx)
    trx = nullify_empty_cells(trx)
    trx = (
        trx.dropDuplicates(columns_for_input_trx)
        .filter(col(conformed.Transactions.idi_counterparty_gr.name).isNotNull())
        .filter(col(conformed.Transactions.idi_proposition.name).isNotNull())
        .filter(col(conformed.Transactions.dat_date_type_1.name).isNotNull())
        .filter(col(conformed.Transactions.cua_amount_type_1.name).isNotNull())
        .cache()
    )

    trx = (
        trx.groupby(
            conformed.Transactions.idi_counterparty_gr.name,
            conformed.Transactions.dat_date_type_1.name,
            conformed.Transactions.idi_proposition.name
        )
        .agg(sum(conformed.Transactions.cua_amount_type_1.name).alias(conformed.Transactions.cua_amount_type_1.name))
    )

    print("2_trx")
    print(trx.count())
    trx.show()

    demo = demo.select(*columns_for_input_demo)
    demo = nullify_empty_cells(demo)
    demo = (
        demo.dropDuplicates(columns_for_input_demo)
        # .filter(col(conformed.Demographics.des_age.name).isNotNull())
        # .filter(col(conformed.Demographics.adr_visit_cde_country.name).isNotNull())
        # .filter(col(conformed.Demographics.ind_person_gender.name).isNotNull())
        .filter(col(conformed.Demographics.idi_counterparty.name).isNotNull())
        .cache()
    )

    print("3_demo")
    print(demo.count())
    demo.show()

    pao = pao.select(*columns_for_input_pao)
    pao = nullify_empty_cells(pao)
    pao = (
        pao.dropDuplicates(columns_for_input_pao)
        .filter(col(features.RfmOutput.idi_counterparty.name).isNotNull())
        .filter(col(features.RfmOutput.customer_segment.name).isNotNull())
        .cache()
    )

    print("4_pao")
    print(pao.count())
    pao.show()

    # # enriching demo with c360 segment from rfm
    # demo = (
    #     demo.alias(conformed.Demographics.table_alias())
    #     .join(
    #         how='left', other=pao.alias(conformed.ProfileAlgorithmsOutput.table_alias()),
    #         on=col(
    #             conformed.Demographics.idi_counterparty.column_alias()) == col(
    #             conformed.ProfileAlgorithmsOutput.idi_counterparty.column_alias()
    #         ))
    #     .drop(col(conformed.ProfileAlgorithmsOutput.idi_counterparty.column_alias()))
    # )

    demo = (
        demo.alias(conformed.Demographics.table_alias())
        .join(
            how='left',
            other=pao.alias(features.RfmOutput.table_alias()),
            on=col(
                conformed.Demographics.idi_counterparty.column_alias()) == col(
                features.RfmOutput.idi_counterparty.column_alias()
            )

        ).drop(col(features.RfmOutput.idi_counterparty.column_alias()))
        )

    print("5_demo segment")
    print(demo.count())
    demo.show()

    # right join trx with demo
    demo = demo.dropDuplicates(["idi_counterparty"])
    demo = demo.withColumnRenamed("idi_counterparty", "idi_counterparty_gr")

    joined_trx = demo.join(trx, on="idi_counterparty_gr", how='right')

    print("6_trx demo")
    print(joined_trx.count())
    joined_trx.show()
    joined_trx = nullify_empty_cells(joined_trx)
    joined_trx = (
        joined_trx.filter(col(conformed.Transactions.idi_counterparty_gr.name).isNotNull())
        .filter(col(conformed.Transactions.idi_proposition.name).isNotNull())
        .filter(col(conformed.Transactions.dat_date_type_1.name).isNotNull())
        .fillna(0, [conformed.Transactions.cua_amount_type_1.name])
    )

    print("7_joined_trx demo")
    print(joined_trx.count())
    joined_trx.show()
    print("7_trx_cat")
    print(trx_cat.count())
    trx_cat.show()

    joined_trx_cat = (
        joined_trx.alias("joined_trx")
        .join(
            how='left', other=trx_cat.alias("trx_cat"), on='idi_proposition'
        )
        .drop("trx_cat.idi_proposition")
    )

    print("8_joined_trx_prod TEMP_02")
    print(joined_trx_cat.count())
    joined_trx_cat.show()

    joined_trx_cat = joined_trx_cat.withColumn("IDI_TRANSACTION",
                                               concat(col(conformed.Transactions.idi_counterparty_gr.name).cast(
                                                   StringType()), lit("_"),
                                                   col(conformed.Transactions.dat_date_type_1.name).cast(
                                                       StringType())))

    joined_trx_cat = joined_trx_cat.withColumn(conformed.Transactions.idi_counterparty.name,
                                               col(conformed.Transactions.idi_counterparty_gr.name))

    joined_trx_cat = joined_trx_cat.select(
        conformed.Transactions.idi_counterparty.name,
        conformed.Transactions.idi_proposition.name,
        conformed.Transactions.dat_date_type_1.name,
        conformed.Transactions.cua_amount_type_1.name,
        conformed.Demographics.des_age.name,
        conformed.Demographics.adr_visit_cde_country.name,
        conformed.Demographics.ind_person_gender.name,
        "CATEGORY",
        "IDI_TRANSACTION",
        features.RfmOutput.customer_segment.name
    )

    # joined_trx_cat = joined_trx_cat.withColumn("CUSTOMER_SEGMENT",
    #                                            col(features.RfmOutput.customer_segment.name)).drop(
    #     features.RfmOutput.customer_segment.name)

    print("9_joined_trx_prod TEMP_DF")
    print(joined_trx_cat.count())
    joined_trx_cat.show()

    return convert_columns_to_upper_case(joined_trx_cat)


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
    demo_df = create_delta_table_from_catalog(spark, conformed.Demographics, lake_descriptor).toDF()
    pao_df = create_delta_table_from_catalog(spark, conformed.ProfileAlgorithmsOutput, lake_descriptor).toDF()
    idmap_df = create_delta_table_from_catalog(spark, conformed.IdMapping, lake_descriptor).toDF()

    sat_data_filter = cvm_data_filter_registry.get_data_filter(data_filter_context)
    trx_df, trx_cat_df, prod_df, demo_df, pao_df, category_filter_columm_present = sat_data_filter(trx=trx_df,
                                                                                                   prod=prod_df,
                                                                                                   demo=demo_df,
                                                                                                   pao=pao_df,
                                                                                                   idmap=idmap_df)
    pao_df = spark.read.csv('s3://cvm-uat-conformed-d5b175d/features/rfm/output/rfm.bu.share/', header=True)

    result = sat_training(trx_df, trx_cat_df, prod_df, demo_df, pao_df)

    print(category_filter_columm_present)
    print(f"Changing result CATEGORY to {category_filter_columm_present}")
    result = result.withColumn(category_filter_columm_present, col("CATEGORY")).drop("CATEGORY")

    if data_filter_context == 'sat.bu.share.proposition':
        result = result.drop(category_filter_columm_present)

    path_to_files = get_s3_path_for_feature(features.SatTrainingInput, lake_descriptor, data_filter_context)
    result.write.mode("overwrite").option("header", "true").csv(path=path_to_files)
