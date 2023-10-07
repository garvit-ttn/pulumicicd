

from pyspark.sql.types import LongType
from pyspark.sql.functions import *

from cvmdatalake import conformed, nullify_empty_cells, create_delta_table_from_catalog, convert_columns_to_upper_case, \
    features, get_s3_path_for_feature, TableSpec, ColumnSpec

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



def filter_for_bu_context(
        trx: DataFrame, prod: DataFrame, trx_filter_column: ColumnSpec, filter_value: str, category: str, data_age_motnhs: int
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


def filter_for_multiple_bus(trx: DataFrame, prod: DataFrame, bu_filters: list[str]) -> Tuple[DataFrame, DataFrame]:
    return (
        trx.filter(col(conformed.Transactions.bu.name).isin(bu_filters)),
        prod.filter(col(conformed.Products.bu.name).isin(bu_filters))
    )


def nbom_training(trx: DataFrame, prod: DataFrame) -> DataFrame:

    columns_for_input_trx = [
        conformed.Transactions.idi_counterparty_gr.name,
        conformed.Transactions.qty_number_of_items.name,
        conformed.Transactions.cua_amount_type_1.name,
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
        .filter(col(conformed.Transactions.qty_number_of_items.name) > 0)
        .filter(col(conformed.Transactions.cua_amount_type_1.name) > 0)
        .withColumn(conformed.Transactions.idi_proposition.name, col("CATEGORY"))
        .select(
            conformed.Transactions.idi_counterparty.name,
            conformed.Transactions.idi_proposition.name,
            conformed.Transactions.qty_number_of_items.name
        )
        .groupby(conformed.Transactions.idi_counterparty.name,conformed.Transactions.idi_proposition.name)
        .agg(sum(conformed.Transactions.qty_number_of_items.name).alias(conformed.Transactions.qty_number_of_items.name))
        # .dropDuplicates()
    )
    return convert_columns_to_upper_case(trx)


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

    result = nbom_training(trx_df, prod_df)

    path_to_files = get_s3_path_for_feature(features.NbomTrainingInput, lake_descriptor, data_filter_context)
    print(path_to_files)

    result.coalesce(100).write.mode("overwrite").option("header", "true").csv(path=path_to_files)




  # {
  #   "Comment": "Insert your JSON here",
  #   "data_filter": {
  #     "context": "nbom.bu.share.proposition03",
  #     "TrainingJobName": "nbom-bu-share-proposition03-train-2023-03-32",
  # 	"TransformJobName": "nbom-bu-share-proposition03-transform-2023-03-32",
  #     "AlgorithmName": "arn:aws:sagemaker:eu-west-1:985815980388:algorithm/maf-recommenderx-dask-rc7-781ad608939636b3a8c4ca212885bd19",
  #     "ModelPackageName": "nbom-bu-share-proposition03-model-pacakge-2023-03-32",
  #     "ModelName": "nbom-bu-share-proposition03-model-2023-03-32",
  #     "model_path": "s3://cvm-uat-conformed-d5b175d/features/nbom/model/nbom.bu.share.proposition03/",
  #     "output_path": "s3://cvm-uat-conformed-d5b175d/features/nbom/output/nbom.bu.share.proposition03/",
  #     "ModelDataUrl": "s3://cvm-uat-conformed-d5b175d/features/nbom/model/nbom.bu.share.proposition03/nbom-bu-share-proposition03-train-2023-03-32/output/model.tar.gz",
  #     "input_train_path": "s3://cvm-uat-conformed-d5b175d/features/nbom/input/train/nbom.bu.share.proposition03/",
  # 	"input_transform_path": "s3://cvm-uat-conformed-d5b175d/features/nbom/input/transform/nbom.bu.share.proposition03/",
  # 	"KmsKeyId": "e65fea64-7da6-4e27-bf4f-9cc2eb54ef0a"
  #   },
  #   "hyperparams": {
  #     "data_filter_context_index": "0",
  #     "values": {
  #       "epochs": "1",
  #       "embedding_dimensions": "32",
  #       "number_of_offers": "5",
  #       "learning_rate": "0.1"
  #     }
  #   }
  # }