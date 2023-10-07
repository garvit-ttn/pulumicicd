from dataclasses import dataclass

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

from cvmdatalake import create_delta_table_from_catalog
from cvmdatalake.conformed import Promotions, ProfileAlgorithmsOutput, Predictions
from cvmdatalake.transformations.id_map import *
from cvmdatalake.transformations.promotions import (
    filter_promotions_for_eligibility, map_promotions_to_details, enrich_with_expiry_column
)


def assign_default_offers(customers: DataFrame, eligible_promos: DataFrame) -> DataFrame:
    eligible_promos = eligible_promos.select(Promotions.idi_offer.name)
    offers_to_load = list(map(lambda x: x.idi_offer, eligible_promos.collect()))

    result = (
        customers
        .withColumn("offers", array(*map(lit, list(offers_to_load))))
        .withColumn('rand_offers', slice(shuffle('offers'), 1, 5)).drop('offers')
        .withColumn(Promotions.idi_offer.name, explode(col('rand_offers'))).drop('rand_offers')
    )

    return result


def format_identifier_and_rank(results: DataFrame, id_src_col: str) -> DataFrame:
    return (
        results
        .withColumn('bu', lit('share'))
        .withColumn('rank', row_number().over(Window.partitionBy(id_src_col).orderBy(id_src_col)))
        .withColumn(id_src_col, format_string("%s#%s", col(id_src_col), col('bu')))
        .groupby(id_src_col).agg(collect_list(struct('rank', col(Promotions.idi_offer.name))).alias('collected_offers'))
        .withColumn('cvm_campaign_name', lit('##default_cvm_campaign_name##'))
        .select(
            id_src_col,
            'cvm_campaign_name',
            col(f'collected_offers.{Promotions.idi_offer.name}').alias(Promotions.idi_offer.name)
        )
    )


@dataclass
class OffersAssignment:
    share: DataFrame
    braze: DataFrame
    offers: DataFrame | None = None

    def merge(self, other):
        return OffersAssignment(
            share=self.share.union(other.share),
            braze=self.braze.union(other.braze),
        )


def load_offers_from_predictions(
        predictions: DataFrame, eligible_promos: DataFrame, id_mapping: DataFrame
) -> tuple[OffersAssignment, DataFrame]:
    # Load only the latest predictions
    latest_prep_date = str(predictions.select(max(col(ProfileAlgorithmsOutput.date_of_prepare.name))).first()[0])
    print(f"Loading predictions for {latest_prep_date}")

    predictions_raw = (
        predictions
        .filter(col(ProfileAlgorithmsOutput.date_of_prepare.name) == latest_prep_date)
        .select([
            ProfileAlgorithmsOutput.idi_counterparty.name,
            ProfileAlgorithmsOutput.nbo_json.name,
            # ProfileAlgorithmsOutput.data_filter_context.name # Only single context is available for now
        ])
    )

    print(f"Prediction total: {predictions.count()}")

    eligible_promos_ids = eligible_promos.select(Promotions.idi_offer.name)
    predictions = ProfileAlgorithmsOutput.alias_df(predictions_raw).join(
        how="inner", other=Promotions.alias_df(eligible_promos_ids),
        on=col(Promotions.idi_offer.alias) == col(ProfileAlgorithmsOutput.nbo_json.alias)
    )

    print(f"Predictions after mapping to eligible offers: {predictions.count()}")

    braze_predictions = enrich_with_customer_src_id(
        predictions, id_mapping, source=SourceSystem.Braze,
        input_idi_gcr_col=ProfileAlgorithmsOutput.idi_counterparty.name, output_src_col='braze_id'
    ).alias(ProfileAlgorithmsOutput.alias()).drop(col(ProfileAlgorithmsOutput.idi_counterparty.alias))

    print(f"Predictions after mapping to braze: {predictions.count()}")

    share_predictions = enrich_with_customer_src_id(
        predictions, id_mapping, source=SourceSystem.Share,
        input_idi_gcr_col=ProfileAlgorithmsOutput.idi_counterparty.name, output_src_col='share_id'
    ).alias(ProfileAlgorithmsOutput.alias()).drop(col(ProfileAlgorithmsOutput.idi_counterparty.alias))

    print(f"Predictions after mapping to share: {predictions.count()}")

    gcr_without_offers_assigned = predictions_raw.select(Predictions.idi_counterparty.name).dropDuplicates()
    print(f"Remaining GCRs without any offers: {gcr_without_offers_assigned.count()}")

    return (
        OffersAssignment(
            share=format_identifier_and_rank(share_predictions, 'share_id'),
            braze=format_identifier_and_rank(braze_predictions, 'braze_id')
        ),
        gcr_without_offers_assigned
    )


def assign_default_offers_for_gcr(
        gcr_without_offers: DataFrame, id_mapping: DataFrame, eligible_promos: DataFrame
) -> OffersAssignment:
    braze_without_offers = enrich_with_customer_src_id(
        gcr_without_offers, id_mapping, source=SourceSystem.Braze,
        input_idi_gcr_col=Predictions.idi_counterparty.name, output_src_col='braze_id'
    ).alias(ProfileAlgorithmsOutput.alias()).drop(col(ProfileAlgorithmsOutput.idi_counterparty.alias))

    share_without_offers = enrich_with_customer_src_id(
        gcr_without_offers, id_mapping, source=SourceSystem.Share,
        input_idi_gcr_col=Predictions.idi_counterparty.name, output_src_col='share_id'
    ).alias(ProfileAlgorithmsOutput.alias()).drop(col(ProfileAlgorithmsOutput.idi_counterparty.alias))

    braze_without_offers = assign_default_offers(braze_without_offers, eligible_promos)
    share_without_offers = assign_default_offers(share_without_offers, eligible_promos)

    print(f"Default Braze offers assigned: {braze_without_offers.count()}")
    print(f"Default Share offers assigned: {share_without_offers.count()}")

    return OffersAssignment(
        braze=format_identifier_and_rank(braze_without_offers, "braze_id"),
        share=format_identifier_and_rank(share_without_offers, "share_id"),
    )


def assign_default_offers_for_share_sample(
        predictions: DataFrame, sample_share_customers: DataFrame, id_mapping: DataFrame,
        eligible_promos: DataFrame) -> DataFrame:
    print(f"Input sample Share consumers: {sample_share_customers.count()}")

    sample_share_customers_with_gcr = enrich_with_customer_gcr_id(
        input_df=sample_share_customers, id_map=id_mapping,
        input_idi_src_col="idi_src", output_gcr_col="gcr_id"
    )

    print(f"Input sample Share consumers after mapping to GCR: {sample_share_customers_with_gcr.count()}")

    sample_share_customers_to_randomize = sample_share_customers_with_gcr.alias('share_customers').join(
        how='left_anti', other=Predictions.alias_df(predictions),
        on=col('share_customers.gcr_id') == col(Predictions.idi_counterparty.alias)
    ).select(col('share_customers.idi_src').alias('share_id'))

    print(f"Sample share customers to randomize: {sample_share_customers_to_randomize.count()}")

    result = assign_default_offers(sample_share_customers_to_randomize, eligible_promos)
    return format_identifier_and_rank(result, "share_id")


def run(
        predictions: DataFrame, promo: DataFrame, id_mapping: DataFrame,
        sample_share_customers: DataFrame) -> OffersAssignment:
    eligible_promos = filter_promotions_for_eligibility(promo)
    eligible_promos_with_details = map_promotions_to_details(eligible_promos)

    predictions_assignment, gcr_without_offers = load_offers_from_predictions(
        predictions, eligible_promos_with_details, id_mapping
    )
    missing_gcr_assignment = assign_default_offers_for_gcr(
        gcr_without_offers, id_mapping, eligible_promos_with_details
    )

    predictions_assignment.share.printSchema()
    predictions_assignment.braze.printSchema()

    missing_gcr_assignment.share.printSchema()
    missing_gcr_assignment.braze.printSchema()

    predictions_assignment = predictions_assignment.merge(missing_gcr_assignment)

    # share_sample_default = assign_default_offers_for_share_sample(
    #     predictions, sample_share_customers, id_mapping, eligible_promos
    # )

    return OffersAssignment(
        offers=eligible_promos,
        braze=predictions_assignment.braze,
        share=predictions_assignment.share
    )


if __name__ == '__main__':
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from awsglue import DynamicFrame

    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'lake_descriptor', 'cvm_environment', 'share_sample_path'])

    sc = SparkContext()
    glueContext = GlueContext(sc)
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    spark: SparkSession = glueContext.spark_session
    lake_descriptor = args['lake_descriptor']
    cvm_environment = args['cvm_environment']
    share_sample_path = args['share_sample_path']

    # noinspection PyTypeChecker
    additional_share_customers = spark.createDataFrame([
        {'idi_src': '9478400000224580'},
        {'idi_src': '9478400024577856'},  # Majd Arzouni
        {'idi_src': '9478400006848762'},  # Saima Ansari
        {'idi_src': '9478400003361439'},  # Payal Uthaiah
        {'idi_src': '9478400023506138'},  # Mario Gebrael
    ])

    print(f"Reading sample Share customers from {share_sample_path}")
    share_customers_df = spark.read.parquet(share_sample_path)
    share_customers_df = share_customers_df.union(additional_share_customers)
    # share_customers_df = additional_share_customers

    predictions_df = create_delta_table_from_catalog(spark, ProfileAlgorithmsOutput, lake_descriptor).toDF()
    promo_df = create_delta_table_from_catalog(spark, Promotions, lake_descriptor).toDF()
    id_mapping_df = create_delta_table_from_catalog(spark, IdMapping, lake_descriptor).toDF()

    print(f"promos: {promo_df.count()}")
    print(f"customers: {share_customers_df.count()}")

    offers_assignment = run(predictions_df, promo_df, id_mapping_df, share_customers_df)

    print(f"Share assignment to upsert: {offers_assignment.share.count()}")
    print(f"Braze assignment to upsert: {offers_assignment.braze.count()}")
    print(f"Offer details to upsert: {offers_assignment.offers.count()}")

    offers_assignment.offers.printSchema()
    # customer_offers = map_promotions_to_details(offers_assignment.offers)
    # glueContext.write_dynamic_frame.from_options(
    #     frame=DynamicFrame.fromDF(customer_offers, glueContext, "customer_offers"),
    #     connection_type="dynamodb",
    #     connection_options={
    #         "tableName": "cvm-offers",
    #         "overwrite": "true"
    #     }
    # )

    eligible_offers = map_promotions_to_details(
        enrich_with_expiry_column(offers_assignment.offers),
        additional_column='expires_at'
    )

    glueContext.write_dynamic_frame.from_options(
        frame=DynamicFrame.fromDF(eligible_offers, glueContext, "eligible_offers"),
        connection_type="dynamodb",
        connection_options={
            "tableName": "cvm-default-eligible-offers",
            "overwrite": "true"
        }
    )

    # TODO: write to these two tables should be split into two separate jobs to parallelize - costs won't change
    # offers_assignment.share.printSchema()
    # glueContext.write_dynamic_frame.from_options(
    #     frame=DynamicFrame.fromDF(offers_assignment.share, glueContext, "result_share"),
    #     connection_type="dynamodb",
    #     connection_options={
    #         "tableName": "cvm-share",
    #         "overwrite": "true"
    #     }
    # )
    #
    # offers_assignment.braze.printSchema()
    # glueContext.write_dynamic_frame.from_options(
    #     frame=DynamicFrame.fromDF(offers_assignment.braze, glueContext, "result_braze"),
    #     connection_type="dynamodb",
    #     connection_options={
    #         "tableName": "cvm-braze",
    #         "overwrite": "true"
    #     }
    # )
