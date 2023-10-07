from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.functions import col, row_number
from pyspark.sql.types import LongType
from pyspark.sql.window import Window
from delta.tables import *

from cvmdatalake.data_filters import register_data_filter, cvm_data_filter_registry, DataFilterContext

from cvmdatalake import conformed, nullify_empty_cells, create_delta_table_from_catalog, convert_columns_to_upper_case, \
    features, get_s3_path, get_s3_path_for_feature, TableSpec



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


def clv_transform(trx: DataFrame) -> DataFrame:
    columns_for_clv_freq_calc = [
        conformed.Transactions.idi_counterparty_gr.name,
        conformed.Transactions.dat_date_type_1.name,
    ]

    columns_for_clv_monet_calc = [
        conformed.Transactions.idi_counterparty_gr.name,
        conformed.Transactions.cua_amount_type_1.name,
        conformed.Transactions.dat_date_type_1.name,
    ]

    columns_for_clv_monet_RECENCY = [
        conformed.Transactions.idi_counterparty_gr.name,
        conformed.Transactions.dat_date_type_1.name
    ]

    columns_for_clv_input = [
        conformed.Transactions.idi_counterparty_gr.name
    ]

    columns_for_clv_monet_t = [
        conformed.Transactions.idi_counterparty_gr.name,
        conformed.Transactions.dat_date_type_1.name,
    ]

    trx = (
        trx.select(*[
            conformed.Transactions.bu.name,
            conformed.Transactions.idi_counterparty_gr.name,
            conformed.Transactions.dat_date_type_1.name,
            conformed.Transactions.cua_amount_type_1.name,
            conformed.Transactions.dat_date_type_1.name
        ])
        .drop(conformed.Transactions.bu.name)
        .cache()
    )
    print("show trx")
    print(trx.show(10, truncate=False))

    trx_FREQ = (
        trx.select(*columns_for_clv_freq_calc)
        .distinct()
        .dropDuplicates(columns_for_clv_freq_calc)
        .groupBy(col(conformed.Transactions.idi_counterparty_gr.name)).agg(
            count(col(conformed.Transactions.dat_date_type_1.name)).alias("FREQ"))
        .withColumn("FREQUENCY", F.col("FREQ") - 1)
        .drop("FREQ")
    )

    trx_MONET = (trx.select(*columns_for_clv_monet_calc).groupBy(
        col(conformed.Transactions.idi_counterparty_gr.name), col(conformed.Transactions.dat_date_type_1.name)).agg(
        sum(col(conformed.Transactions.cua_amount_type_1.name)).alias("SUM_MONETARY")).groupBy(
        col(conformed.Transactions.idi_counterparty_gr.name)).agg(
        avg("SUM_MONETARY").alias("MONETARY"))
    )

    # trx_MONET_pd = trx_MONET.toPandas()

    trx_Dept_desc = (Window.partitionBy(col(conformed.Transactions.idi_counterparty_gr.name))
                     .orderBy(col(conformed.Transactions.dat_date_type_1.name).desc())
                     )

    trx_Dept_asc = (Window.partitionBy(col(conformed.Transactions.idi_counterparty_gr.name))
                    .orderBy(col(conformed.Transactions.dat_date_type_1.name).asc())
                    )

    # TODO LAST TO FIRST DIFF DATE
    trx_LAST = (
        trx.select(*columns_for_clv_monet_RECENCY)
        .withColumn("row", row_number().over(trx_Dept_desc))
        .filter(col("row") == 1)
        .drop("row")
        .withColumn("LAST", col(conformed.Transactions.dat_date_type_1.name))
        .drop(col(conformed.Transactions.dat_date_type_1.name))
    )

    # trx_LAST_pd = trx_LAST.toPandas()

    trx_FIRST = (
        trx.select(*columns_for_clv_monet_RECENCY)
        .withColumn("row", row_number().over(trx_Dept_asc))
        .filter(col("row") == 1)
        .drop("row")
        .withColumn("FIRST", col(conformed.Transactions.dat_date_type_1.name))
        .drop(col(conformed.Transactions.dat_date_type_1.name))
    )

    # trx_FIRST_pd = trx_LAST.toPandas()

    trx_RECENCY = trx.select(*columns_for_clv_input)
    trx_RECENCY = nullify_empty_cells(trx_RECENCY)
    trx_RECENCY = (
        trx_RECENCY.dropDuplicates()
        .filter(col(conformed.Transactions.idi_counterparty_gr.name).isNotNull())
    )

    trx_RECENCY = (
        trx_RECENCY
        .join(trx_LAST, conformed.Transactions.idi_counterparty_gr.name, 'left')
        .join(trx_FIRST, conformed.Transactions.idi_counterparty_gr.name, 'left')
        .withColumn("RECENCY", datediff(col("LAST"), col("FIRST")))
        .drop(col("FIRST"))
        .drop(col("LAST"))
    )

    # trx_RECENCY_pd = trx_RECENCY.toPandas()

    trx_T = (
        trx.select(*columns_for_clv_monet_t)
        .withColumn("row", row_number().over(trx_Dept_asc))
        .filter(col("row") == 1)
        .drop("row")
        .withColumn("T", datediff(current_date(), col(conformed.Transactions.dat_date_type_1.name)))
        .drop(col(conformed.Transactions.dat_date_type_1.name))
    )

    trx = trx.select(*columns_for_clv_input)
    trx = nullify_empty_cells(trx)
    trx = (
        trx.dropDuplicates()
        .filter(col(conformed.Transactions.idi_counterparty_gr.name).isNotNull())
    )

    gcr_joined_trx_F = trx.join(trx_FREQ, conformed.Transactions.idi_counterparty_gr.name, 'left')
    gcr_joined_trx_FR = gcr_joined_trx_F.join(trx_RECENCY, conformed.Transactions.idi_counterparty_gr.name, 'left')
    gcr_joined_trx_FRT = gcr_joined_trx_FR.join(trx_T, conformed.Transactions.idi_counterparty_gr.name, 'left')
    gcr_joined_trx_FRTM = gcr_joined_trx_FRT.join(trx_MONET, conformed.Transactions.idi_counterparty_gr.name, 'left')

    gcr_joined_trx_FRTM = (
        gcr_joined_trx_FRTM
        .filter(col(conformed.Transactions.idi_counterparty_gr.name).isNotNull())
        .withColumn(
            conformed.Transactions.idi_counterparty_gr.name,
            col(conformed.Transactions.idi_counterparty_gr.name).cast(StringType())
        )
        .drop('count')
        .filter(col(conformed.Transactions.idi_counterparty_gr.name).cast(LongType()).isNotNull())
    )

    gcr_joined_trx_FRTM = (gcr_joined_trx_FRTM.drop(conformed.Transactions.idi_counterparty.name).withColumn(
        conformed.Transactions.idi_counterparty.name, col(conformed.Transactions.idi_counterparty_gr.name)).drop(
        conformed.Transactions.idi_counterparty_gr.name))

    gcr_joined_trx_FRTM = convert_columns_to_upper_case(gcr_joined_trx_FRTM)

    gcr_joined_trx_FRTM = gcr_joined_trx_FRTM.select("IDI_COUNTERPARTY", "FREQUENCY", "RECENCY", "T", "MONETARY")

    print(gcr_joined_trx_FRTM.show(10, truncate=False))

    return convert_columns_to_upper_case(gcr_joined_trx_FRTM)


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
    print(data_filter_context)

    trx_df = create_delta_table_from_catalog(spark, conformed.Transactions, lake_descriptor).toDF()
    prod_df = create_delta_table_from_catalog(spark, conformed.Products, lake_descriptor).toDF()

    clv_data_filter = cvm_data_filter_registry.get_data_filter(data_filter_context)
    trx_df, prod_df = clv_data_filter(trx=trx_df, prod=prod_df)
    trx_df.show()

    path_to_files = get_s3_path_for_feature(features.ClvTransformInput, lake_descriptor, data_filter_context)
    print(path_to_files)

    result = clv_transform(trx_df)
    repartition_count = 1

    if result.count() > 10000:
        repartition_count = int(result.count() / 10000)

    result.repartition(repartition_count).write.mode("overwrite").option("header", "true").csv(
        path=path_to_files)
