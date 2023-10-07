from collections import namedtuple
import json

import pulumi
from pulumi_aws.glue import Job

from cvmdatalake import Stage
from cvmdatalake.pulumi import enable_debug
from deploy import cvm_glue_job, cvm_workflow, cvm_lake_stack, cvm_stack, cvm_lambda_function

import algorithms.clv.transform_flow
import algorithms.clv.train_flow

import algorithms.mba.transform_flow
import algorithms.mba.train_flow

import algorithms.rfm.transform_flow
import algorithms.rfm.train_flow

import algorithms.sat.transform_flow
import algorithms.sat.train_flow

import algorithms.nbo.transform_flow
import algorithms.nbo.train_flow

import algorithms.nbom.transform_flow
import algorithms.nbom.train_flow

from workflows import (
    ingestion_flow, braze_audience_flow, promotions_workflow, customer_profiles_workflow,
    refresh_profiles_workflow, bu_external_id_workflow, braze_new_audience_flow, share_audiences_workflow
)

from flexible_profile import (
    prepare_data_for_customer_profiles, train_clv_models_for_customer_profiles,
    train_nbo_models_for_customer_profiles, train_rfm_models_for_customer_profiles, run_all
)

from cvmdatalake import landing, staging, conformed
from cvmdatalake.workflows import workflow_lift
from cvmdatalake.pulumi import transform_descriptor, get_crawler_name

from extensions.maf.apis.dynamodb import *

enable_debug(flag=False)

lake_stack = cvm_lake_stack()
base_stack = cvm_stack('base')

config = pulumi.Config()
environment = config.require("environment")

sm_exec_role = base_stack.get_output("sm-exec-role-arn")
lake_descriptor = lake_stack.get_output('lake_descriptor').apply(transform_descriptor)
descriptor = lake_descriptor.apply(lambda d: json.dumps(dict(sorted(d.tables.items()))))
kms_key_id = aws.kms.get_key(key_id="alias/cvm-sagemaker").id

prep_transactions_job = cvm_glue_job(
    job_path="ingestion/maf_data_prep/transform_transactions.py",
    description="This job is responsible for preparing transactions loading from old PoC data",
    stage=Stage.Ingestion, lake_stack=lake_stack, base_stack=base_stack,
    prefix=environment, enable_bookmarks=False
)

prep_id_mapping_job = cvm_glue_job(
    job_path="ingestion/maf_data_prep/transform_id_mapping.py",
    description="This job is responsible for preparing id mapping table based on the old PoC data",
    stage=Stage.Ingestion, lake_stack=lake_stack, base_stack=base_stack,
    prefix=environment, enable_bookmarks=False
)

prep_demographics_job = cvm_glue_job(
    job_path="ingestion/maf_data_prep/transform_demographics.py",
    description="Preprocessing job to fix all inconsistencies in the sample demographics provided by Vipin Kumar",
    stage=Stage.Ingestion, lake_stack=lake_stack, base_stack=base_stack,
    prefix=environment, enable_bookmarks=False
)

prep_algos_features_job = cvm_glue_job(
    job_path="algorithms/features_prep.py",
    description="Rudimentary job to prep basic feature to run algos for the demo purpose",
    stage=Stage.Ingestion, lake_stack=lake_stack, base_stack=base_stack,
    prefix=environment, enable_bookmarks=False)

id_mapping_ingestion_job = cvm_glue_job(
    job_path="ingestion/id_mapping_ingestion.py",
    description="This job is responsible for ingesting IDs mapping data from landing to staging",
    stage=Stage.Ingestion, lake_stack=lake_stack, base_stack=base_stack,
    prefix=environment, enable_bookmarks=True
)

transactions_ingestion_job = cvm_glue_job(
    job_path="ingestion/transactions_ingestion.py",
    description="This job is responsible for ingesting transactions data from landing to staging",
    stage=Stage.Ingestion, lake_stack=lake_stack, base_stack=base_stack,
    prefix=environment, enable_bookmarks=True, worker_type='G.2X', number_of_workers=40
)

activities_ingestion_job = cvm_glue_job(
    job_path="ingestion/activities_ingestion.py",
    description="This job is responsible for merging validate data from staging to conformed",
    stage=Stage.Ingestion, lake_stack=lake_stack, base_stack=base_stack,
    prefix=environment, enable_bookmarks=True)

demographics_ingestion_job = cvm_glue_job(
    job_path="ingestion/demographics_ingestion.py",
    description="This job is responsible for performing data ingestion from landing to staging",
    stage=Stage.Ingestion, lake_stack=lake_stack, base_stack=base_stack, number_of_workers=30,
    prefix=environment, enable_bookmarks=True
)

products_ingestion_job = cvm_glue_job(
    job_path="ingestion/products_ingestion.py",
    description="This job is responsible for performing data ingestion from landing to staging ",
    stage=Stage.Ingestion, lake_stack=lake_stack, base_stack=base_stack,
    prefix=environment, enable_bookmarks=True, number_of_workers=20
)

promotions_ingestion_job = cvm_glue_job(
    job_path="ingestion/promotions_ingestion.py",
    description="This job is responsible for performing data ingestion from landing to staging",
    stage=Stage.Ingestion, lake_stack=lake_stack, base_stack=base_stack,
    prefix=environment, enable_bookmarks=False
)

custom_attributes_ingestion_job = cvm_glue_job(
    job_path="ingestion/custom_attributes_ingestion.py",
    description="This job is responsible for performing Custom (Braze) attributes data ingestion from landing to staging",
    stage=Stage.Ingestion, lake_stack=lake_stack, base_stack=base_stack, number_of_workers=100,
    prefix=environment, enable_bookmarks=True
)

location_ingestion_job = cvm_glue_job(
    job_path="ingestion/location_ingestion.py",
    description="This job is responsible for performing data ingestion from landing to staging",
    stage=Stage.Ingestion, lake_stack=lake_stack, base_stack=base_stack,
    prefix=environment, enable_bookmarks=True
)

bu_external_id_ingestion_job = cvm_glue_job(
    job_path="ingestion/bu_external_id_ingestion.py",
    description="This job is responsible for performing data ingestion from landing to staging",
    stage=Stage.Ingestion, lake_stack=lake_stack, base_stack=base_stack,
    prefix=environment, enable_bookmarks=True
)

loyalty_ingestion_ingestion_job = cvm_glue_job(
    job_path="ingestion/loyalty_ingestion.py",
    description="This job is responsible for performing data ingestion from landing to staging",
    stage=Stage.Ingestion, lake_stack=lake_stack, base_stack=base_stack,
    prefix=environment, enable_bookmarks=True
)

activities_merge_job = cvm_glue_job(
    job_path="ingestion/activities_merge.py",
    description="This job is responsible for merging validate data from staging to conformed",
    stage=Stage.Ingestion, lake_stack=lake_stack, base_stack=base_stack,
    prefix=environment
)

id_mappings_merging_job = cvm_glue_job(
    job_path="ingestion/id_mapping_merge.py",
    description="This job is responsible for merging validate ID mapping from staging to conformed",
    stage=Stage.Ingestion, lake_stack=lake_stack, base_stack=base_stack,
    prefix=environment
)

demographics_merging_job = cvm_glue_job(
    job_path="ingestion/demographics_merge.py",
    description="This job is responsible for merging validate data from staging to conformed",
    stage=Stage.Ingestion, lake_stack=lake_stack, base_stack=base_stack,
    prefix=environment
)

promotions_merging_job = cvm_glue_job(
    job_path="ingestion/promotions_merge.py",
    description="This job is responsible for merging validate data from staging to conformed",
    stage=Stage.Ingestion, lake_stack=lake_stack, base_stack=base_stack,
    prefix=environment
)

bu_external_id_merging_job = cvm_glue_job(
    job_path="ingestion/bu_external_id_merge.py",
    description="This job is responsible for merging validate data from staging to conformed",
    stage=Stage.Ingestion, lake_stack=lake_stack, base_stack=base_stack,
    prefix=environment
)

products_merging_job = cvm_glue_job(
    job_path="ingestion/products_merge.py",
    description="This job is responsible for merging validate data from staging to conformed",
    stage=Stage.Ingestion, lake_stack=lake_stack, base_stack=base_stack, number_of_workers=30,
    prefix=environment
)

transactions_merging_job = cvm_glue_job(
    job_path="ingestion/transactions_merge.py",
    description="This job is responsible for merging transactions data from staging to conformed",
    stage=Stage.Ingestion, lake_stack=lake_stack, base_stack=base_stack, worker_type='G.2X', number_of_workers=100,
    prefix=environment
)

custom_attributes_merging_job = cvm_glue_job(
    job_path="ingestion/custom_attributes_merge.py",
    description="This job is responsible for merging Custom (Braze) attributes from staging to conformed",
    stage=Stage.Ingestion, lake_stack=lake_stack, base_stack=base_stack, number_of_workers=100,
    prefix=environment
)

location_merging_job = cvm_glue_job(
    job_path="ingestion/location_merge.py",
    description="This job is responsible for merging validate data from staging to conformed",
    stage=Stage.Ingestion, lake_stack=lake_stack, base_stack=base_stack,
    prefix=environment
)

loyalty_merging_job = cvm_glue_job(
    job_path="ingestion/loyalty_merge.py",
    description="This job is responsible for merging validate data from staging to conformed",
    stage=Stage.Ingestion, lake_stack=lake_stack, base_stack=base_stack,
    prefix=environment
)

profile_measurements_ingestion = cvm_glue_job(
    job_path="ingestion/profile_measurements_ingestion.py",
    description="This job is responsible ingesting profile_measurements. Part of pipeline.",
    stage=Stage.Ingestion, lake_stack=lake_stack, base_stack=base_stack,
    prefix=environment
)

share_audience_ingestion = cvm_glue_job(
    job_path="ingestion/share_audience_ingestion.py",
    description="This job is responsible ingesting share_audience. Part of pipeline.",
    stage=Stage.Ingestion, lake_stack=lake_stack, base_stack=base_stack,
    prefix=environment
)

share_audience_merging_job = cvm_glue_job(
    job_path="ingestion/share_audience_merge.py",
    description="This job is responsible for merging validate data from staging to conformed",
    stage=Stage.Ingestion, lake_stack=lake_stack, base_stack=base_stack,
    prefix=environment
)

load_offers_to_bank_job = cvm_glue_job(
    job_path="offers_bank/load_promotions_to_offers_bank.py",
    description="This job is responsible for loading promotions from conformed layer into Offers Bank",
    stage=Stage.ResultsExtract, lake_stack=lake_stack, base_stack=base_stack,
    prefix=environment, number_of_workers=2, worker_type='G.2X',
    connection=base_stack.get_output('rds_cvm_connection_name'),
    additional_job_args={
        'cvm_rds_db_name': base_stack.get_output("cvm-offers-db-name"),
        'cvm_offers_db_password_secret_name': base_stack.get_output("cvm-offers-db-password-secret-name"),
        'rds_host': base_stack.get_output("rds_host"),
    }
)

test_mysql_connction_job = cvm_glue_job(
    job_path="offers_bank/mysql_connection.py",
    description="This job is responsible for loading promotions from conformed layer into Offers Bank",
    stage=Stage.ResultsExtract, lake_stack=lake_stack, base_stack=base_stack,
    prefix=environment, number_of_workers=2, worker_type='G.2X',
    connection=base_stack.get_output('rds_cvm_connection_name'),
    additional_job_args={
        'cvm_rds_db_name': base_stack.get_output("cvm-offers-db-name"),
        'cvm_offers_db_password_secret_name': base_stack.get_output("cvm-offers-db-password-secret-name"),
        'rds_host': base_stack.get_output("rds_host"),
    }
)

load_cdp_results_job = cvm_glue_job(
    job_path="offers_bank/load_cdp_result_to_offers_bank.py",
    description="This job is responsible for loading cdp_results from Customer Profiles",
    stage=Stage.ResultsExtract, lake_stack=lake_stack, base_stack=base_stack,
    prefix=environment, number_of_workers=2, worker_type='G.2X',
    connection=base_stack.get_output('rds_cvm_connection_name'),
    additional_job_args={
        'cvm_rds_db_name': base_stack.get_output("cvm-offers-db-name"),
        'cvm_offers_db_password_secret_name': base_stack.get_output("cvm-offers-db-password-secret-name"),
        'rds_host': base_stack.get_output("rds_host"),
    }
)

c360_profiles_creation_job_new = cvm_glue_job(
    job_path="ingestion/c360.py",
    description="This job is responsible for creating C360 profiles from demographics data",
    stage=Stage.Ingestion, lake_stack=lake_stack, base_stack=base_stack, worker_type='G.4X', number_of_workers=10,
    prefix=environment
)

calculated_kpi = cvm_glue_job(
    job_path="ingestion/calculated_kpi.py",
    description="This job is responsible for creating calculated kpi", stage=Stage.Ingestion,
    lake_stack=lake_stack, base_stack=base_stack, prefix=environment,
    worker_type='G.8X', number_of_workers=20,
)

export_audience_info = cvm_glue_job(
    job_path="extensions/maf/braze_push/export_audience_info.py",
    description="This job is responsible for loading cdp_results from Customer Profiles",
    stage=Stage.ResultsExtract, lake_stack=lake_stack, base_stack=base_stack,
    prefix=environment, number_of_workers=2,
    connection=base_stack.get_output('rds_cvm_connection_name')
)

calculate_cdp_results_stat = cvm_glue_job(
    job_path="offers_bank/calculate_cdp_results_stats.py",
    description="This job is responsible for precalculating stats from Customer Profiles",
    stage=Stage.ResultsExtract, lake_stack=lake_stack, base_stack=base_stack,
    prefix=environment,
    connection=base_stack.get_output('rds_cvm_connection_name'),
    additional_job_args={
        'cvm_rds_db_name': base_stack.get_output("cvm-offers-db-name"),
        'cvm_offers_db_password_secret_name': base_stack.get_output("cvm-offers-db-password-secret-name"),
        'rds_host': base_stack.get_output("rds_host"),
    }
)

refresh_audience_stat = cvm_glue_job(
    job_path="offers_bank/refresh_audiences_stats.py",
    description="This job is responsible for refreshing audience count and CLV for RDS",
    stage=Stage.ResultsExtract, lake_stack=lake_stack, base_stack=base_stack,
    prefix=environment,
    connection=base_stack.get_output('rds_cvm_connection_name'),
    additional_job_args={
        'cvm_rds_db_name': base_stack.get_output("cvm-offers-db-name"),
    }
)

cmt_data_prep = cvm_glue_job(
    job_path="offers_bank/cmt_audiences_results.py",
    description="This job is responsible for getting 'idi_counterparty' for each audience",
    stage=Stage.ResultsExtract, lake_stack=lake_stack, base_stack=base_stack,
    prefix=environment,
    connection=base_stack.get_output('rds_cvm_connection_name'),
    additional_job_args={
        'cvm_rds_db_name': base_stack.get_output("cvm-offers-db-name"),
    }
)

if environment in ['dev', 'uat', 'prod']:
    offers_table = create_offers_table(environment)
    default_eligible_offers_table = create_default_eligible_offers_table(environment)
    share_table = create_share_table(environment)
    braze_table = create_braze_table(environment)
    unified_offers_table = create_unified_offers_table(environment)

# Others
load_next_best_offers_job = cvm_glue_job(
    job_path="extensions/maf/apis/load_next_best_offers.py",
    description="This job is responsible for loading next best offers from Customer Profiles to Unified API database",
    stage=Stage.ApiExtract, lake_stack=lake_stack, base_stack=base_stack, prefix=environment, number_of_workers=5,
    additional_job_args={
        'share_sample_path': (
            's3://cvm-prod-conformed-dd6241f/share_customers.parquet' if environment == 'prod'
            else 's3://cvm-uat-conformed-d5b175d/share_customers.parquet'
        )
    }
)

offers_load_job = cvm_glue_job(
    job_path="ingestion/offers_load.py",
    description="This job is responsible for loading offers from Offer Bank",
    stage=Stage.Ingestion, lake_stack=lake_stack, base_stack=base_stack, prefix=environment
)

# control tower

cmt_load_job = cvm_glue_job(
    job_path="control_tower/load_cmt_results.py",
    description="This job is responsible for loading cmt activities to tableau ",
    stage=Stage.ControlTowerExtract, lake_stack=lake_stack, base_stack=base_stack,
    prefix=environment,
    connection=base_stack.get_output('rds_cvm_connection_name'),
    additional_job_args={
        'cvm_rds_db_name': base_stack.get_output("cmv-base:cvm-tableau-db-name"),
    }
)

data_validation_dashboard_load_job = cvm_glue_job(
    job_path="control_tower/load_data_validation_dashboard.py",
    description="This job is responsible for loading data validation dashboard tableau",
    stage=Stage.ControlTowerExtract, lake_stack=lake_stack, base_stack=base_stack,
    prefix=environment,
    connection=base_stack.get_output('rds_cvm_connection_name'),
    additional_job_args={
        'cvm_rds_db_name': base_stack.get_output("cvm-offers-db-name"),
    }
)

data_validation_dashboard_calculate_job = cvm_glue_job(
    job_path="control_tower/calculate_data_validation_dashboard.py",
    description="This job is responsible for calculating data validation dashboard and save output to conformed DataValidationDashboard",
    stage=Stage.ControlTowerExtract, lake_stack=lake_stack, base_stack=base_stack,
    prefix=environment
)

# Braze Audience
audience_builder_job = cvm_glue_job(
    job_path="extensions/maf/braze_push/audience_builder.py",
    description="This job is responsible for creating C360 profiles from demographics data",
    stage=Stage.ApiExtract, lake_stack=lake_stack, base_stack=base_stack,
    prefix=environment, connection=base_stack.get_output('rds_cvm_connection_name'),
    number_of_workers=5,
    additional_job_args={
        'cvm_rds_db_name': base_stack.get_output("cvm-offers-db-name")
    }
)
braze_build_audience = cvm_glue_job(
    job_path="extensions/maf/braze_push/exclusion_code.py",
    description="This job is responsible for creating C360 profiles from demographics data",
    stage=Stage.ApiExtract, lake_stack=lake_stack, base_stack=base_stack,
    prefix=environment, connection=base_stack.get_output('rds_cvm_connection_name'),
    number_of_workers=5,
    additional_job_args={
        'cvm_rds_db_name': base_stack.get_output("cvm-offers-db-name")
    }
)
targetted_offers_exclusion = cvm_glue_job(
    job_path="extensions/maf/braze_push/targetted_offers_exclusion.py",
    description="This job is responsible for creating C360 profiles from demographics data",
    stage=Stage.ApiExtract, lake_stack=lake_stack, base_stack=base_stack,
    prefix=environment, connection=base_stack.get_output('rds_cvm_connection_name'),
    number_of_workers=5,
    additional_job_args={
        'cvm_rds_db_name': base_stack.get_output("cvm-offers-db-name")
    }
)

delta_build_audience = cvm_glue_job(
    job_path="extensions/maf/braze_push/delta_audience_builder.py",
    description="This job is responsible for creating C360 profiles from demographics data",
    stage=Stage.ApiExtract, lake_stack=lake_stack, base_stack=base_stack,
    prefix=environment, connection=base_stack.get_output('rds_cvm_connection_name'),
    number_of_workers=5,
    additional_job_args={
        'cvm_rds_db_name': base_stack.get_output("cvm-offers-db-name")
    }
)

cvm_campaign_name_graphql = cvm_glue_job(
    job_path="extensions/maf/braze_push/delta_audience_copy.py",
    description="This job is responsible for adding cvm_Campaign_name in dynamodb",
    stage=Stage.ApiExtract, lake_stack=lake_stack, base_stack=base_stack,
    prefix=environment, connection=base_stack.get_output('rds_cvm_connection_name'),
    number_of_workers=5,
    additional_job_args={
        'cvm_rds_db_name': base_stack.get_output("cvm-offers-db-name")
    }
)

braze_build_offers_for_audience = cvm_glue_job(
    job_path="extensions/maf/braze_push/braze_build_offers_for_audience.py",
    description="This job is responsible for creating C360 profiles from demographics data",
    stage=Stage.ApiExtract, lake_stack=lake_stack, base_stack=base_stack,
    prefix=environment, connection=base_stack.get_output('rds_cvm_connection_name'),
    number_of_workers=5,
    additional_job_args={
        'cvm_rds_db_name': base_stack.get_output("cvm-offers-db-name")
    }
)

braze_push_audiences_job = cvm_glue_job(
    job_path="extensions/maf/braze_push/audiences.py",
    description="This job is responsible for pushing audiences from Customer Profiles to Braze",
    stage=Stage.ApiExtract, lake_stack=lake_stack, base_stack=base_stack,
    prefix=environment, number_of_workers=5,
    additional_job_args={
        'cvm_maf_braze_sync_api_key_secret_name': base_stack.get_output("cvm-braze-push-auth-secret_name"),
        'cvm_maf_braze_sync_api_auth_url': base_stack.get_output("cvm-braze-push-auth-url"),
        'cvm_maf_braze_sync_api_push_url': base_stack.get_output("cvm-braze-push-url"),
    }
)
braze_push_audiences_new_job = cvm_glue_job(
    job_path="extensions/maf/braze_push/audiences_new.py",
    description="This job is responsible for pushing audiences from Customer Profiles to Braze",
    stage=Stage.ApiExtract, lake_stack=lake_stack, base_stack=base_stack,
    prefix=environment, number_of_workers=5,
    additional_job_args={
        'cvm_maf_braze_sync_api_key_secret_name': base_stack.get_output("cvm-braze-push-auth-secret_name"),
        'cvm_maf_braze_sync_api_auth_url': base_stack.get_output("cvm-braze-push-auth-url"),
        'cvm_maf_braze_sync_api_push_url': base_stack.get_output("cvm-braze-push-url"),
    }
)

uat_prep = cvm_glue_job(
    job_path="ingestion/maf_data_prep/uat_profiles.py",
    description="This job is responsible preparing test profiles for UAT", stage=Stage.Ingestion,
    lake_stack=lake_stack, base_stack=base_stack, prefix=environment, number_of_workers=20
)

AlgoWorkflow = namedtuple("AlgoWorkflow", "train transform")


def deploy_algorithm_workflow(
        algo: str, prep_train_features: Job, prep_transform_features: Job, output_to_pred: Job,
        train_gen, transform_gen
) -> AlgoWorkflow:
    algo_transform_flow_gen = transform_gen(
        lake_stack=lake_stack,
        feature_preparation_job_name=prep_transform_features.name,
        output_to_predictions_job_name=output_to_pred.name
    )

    algo_train_flow_gen = train_gen(
        lake_stack=lake_stack,
        feature_preparation_job_name=prep_train_features.name,
        sm_execution_role=sm_exec_role,
    )

    algo_transform_workflow = cvm_workflow(
        name=f"cmv_{environment}_{algo}_transform_workflow",
        definition_gen=algo_transform_flow_gen,
        stage=Stage.Flexible
    )

    algo_train_workflow = cvm_workflow(
        name=f"cmv_{environment}_{algo}_train_workflow",
        definition_gen=algo_train_flow_gen,
        stage=Stage.Flexible
    )

    return algo_train_workflow, algo_transform_workflow


# clv flexible
prepare_transform_features_for_clv_job = cvm_glue_job(
    job_path="algorithms/clv/prepare_features_for_transform.py",
    description="This job is responsible for preparing transform/inference features for CLV",
    stage=Stage.Ingestion, lake_stack=lake_stack,
    base_stack=base_stack, prefix=environment, max_concurrent_runs=30
)

prepare_features_for_training_for_clv_job = cvm_glue_job(
    job_path="algorithms/clv/prepare_features_for_training.py",
    description="This job is responsible for preparing train data for CLV algorithm",
    stage=Stage.Ingestion, lake_stack=lake_stack,
    base_stack=base_stack, prefix=environment, worker_type='G.1X', max_concurrent_runs=30
)

clv_output_to_predictions_job = cvm_glue_job(
    job_path="algorithms/clv/output_to_predictions_job.py",
    description="This job take flexible output for CLV and add data to the ProfileAlgorithmsOutput table",
    stage=Stage.Ingestion, lake_stack=lake_stack, base_stack=base_stack, prefix=environment, max_concurrent_runs=30
)

reload_clv_job = cvm_glue_job(
    job_path="algorithms/clv/reload_clv.py",
    description="Reload CLV from other profiles table",
    stage=Stage.Ingestion, lake_stack=lake_stack, base_stack=base_stack, prefix=environment
)

clv_train_flow, clv_transform_flow = deploy_algorithm_workflow(
    algo='clv',
    prep_train_features=prepare_features_for_training_for_clv_job,
    prep_transform_features=prepare_transform_features_for_clv_job,
    output_to_pred=clv_output_to_predictions_job,
    train_gen=algorithms.clv.train_flow.generate_workflow_definition_lift,
    transform_gen=algorithms.clv.transform_flow.generate_workflow_definition_lift
)

# mba flexible
prepare_transform_features_for_mba_job = cvm_glue_job(
    job_path="algorithms/mba/prepare_features_for_transform.py",
    description="This job is responsible for preparing transform/inference features for MBA",
    stage=Stage.Ingestion, lake_stack=lake_stack,
    base_stack=base_stack, prefix=environment,
)

prepare_features_for_training_for_mba_job = cvm_glue_job(
    job_path="algorithms/mba/prepare_features_for_training.py",
    description="This job is responsible for preparing train data for MBA algorithm",
    stage=Stage.Ingestion, lake_stack=lake_stack,
    base_stack=base_stack, prefix=environment, worker_type='G.2X',
)

mba_output_to_predictions_job = cvm_glue_job(
    job_path="algorithms/mba/output_to_predictions_job.py",
    description="This job take flexible output for MBA and add data to the Profile Algorithms Output table",
    stage=Stage.Ingestion, lake_stack=lake_stack, base_stack=base_stack, prefix=environment
)

mba_train_flow, mba_transform_flow = deploy_algorithm_workflow(
    algo='mba',
    prep_train_features=prepare_features_for_training_for_mba_job,
    prep_transform_features=prepare_transform_features_for_mba_job,
    output_to_pred=mba_output_to_predictions_job,
    train_gen=algorithms.mba.train_flow.generate_workflow_definition_lift,
    transform_gen=algorithms.mba.transform_flow.generate_workflow_definition_lift
)

# nbo flexible
prepare_transform_features_for_nbo_job = cvm_glue_job(
    job_path="algorithms/nbo/prepare_features_for_transform.py",
    description="This job is responsible for preparing transform/inference features for NBO",
    stage=Stage.Ingestion, lake_stack=lake_stack,
    base_stack=base_stack, prefix=environment, worker_type='G.2X',
)

prepare_training_features_for_nbo_job = cvm_glue_job(
    job_path="algorithms/nbo/prepare_features_for_training.py",
    description="This job is responsible for preparing training features for NBO",
    stage=Stage.Ingestion, lake_stack=lake_stack,
    base_stack=base_stack, prefix=environment, worker_type='G.1X',
)

nbo_output_to_predictions_job = cvm_glue_job(
    job_path="algorithms/nbo/output_to_predictions_job.py",
    description="This job take flexible output for MBA and add data to the Profile Algorithms Output table",
    stage=Stage.Ingestion, lake_stack=lake_stack, base_stack=base_stack, prefix=environment
)

nbo_train_flow, nbo_transform_flow = deploy_algorithm_workflow(
    algo='nbo',
    prep_train_features=prepare_training_features_for_nbo_job,
    prep_transform_features=prepare_transform_features_for_nbo_job,
    output_to_pred=nbo_output_to_predictions_job,
    train_gen=algorithms.nbo.train_flow.generate_workflow_definition_lift,
    transform_gen=algorithms.nbo.transform_flow.generate_workflow_definition_lift
)

# sat flexible
prepare_transform_features_for_sat_job = cvm_glue_job(
    job_path="algorithms/sat/prepare_features_for_transform.py",
    description="This job is responsible for preparing transform/inference features for sat",
    stage=Stage.Ingestion, lake_stack=lake_stack,
    base_stack=base_stack, prefix=environment,
)

prepare_features_for_training_for_sat_job = cvm_glue_job(
    job_path="algorithms/sat/prepare_features_for_training.py",
    description="This job is responsible for preparing train data for sat algorithm",
    stage=Stage.Ingestion, lake_stack=lake_stack,
    base_stack=base_stack, prefix=environment, worker_type='G.1X',
)

sat_output_to_predictions_job = cvm_glue_job(
    job_path="algorithms/sat/output_to_predictions_job.py",
    description="This job take flexible output for SAT and add data to the ProfileAlgorithmsOutput table",
    stage=Stage.Ingestion, lake_stack=lake_stack, base_stack=base_stack, prefix=environment
)

sat_train_flow, sat_transform_flow = deploy_algorithm_workflow(
    algo='sat',
    prep_train_features=prepare_features_for_training_for_sat_job,
    prep_transform_features=prepare_transform_features_for_sat_job,
    output_to_pred=sat_output_to_predictions_job,
    train_gen=algorithms.sat.train_flow.generate_workflow_definition_lift,
    transform_gen=algorithms.sat.transform_flow.generate_workflow_definition_lift
)

# rfm flexible
prepare_transform_features_for_rfm_job = cvm_glue_job(
    job_path="algorithms/rfm/prepare_features_for_transform.py",
    description="This job is responsible for preparing transform/inference features for RFM",
    stage=Stage.Ingestion, lake_stack=lake_stack,
    base_stack=base_stack, prefix=environment, worker_type='G.1X', max_concurrent_runs=30
)

prepare_training_features_for_rfm_job = cvm_glue_job(
    job_path="algorithms/rfm/prepare_features_for_training.py",
    description="This job is responsible for preparing training features for RFM",
    stage=Stage.Ingestion, lake_stack=lake_stack,
    base_stack=base_stack, prefix=environment, max_concurrent_runs=30
)

rfm_output_to_predictions_job = cvm_glue_job(
    job_path="algorithms/rfm/output_to_predictions_job.py",
    description="This job take flexible output for RFM and add data to the ProfileAlgorithmsOutput table",
    stage=Stage.Ingestion, lake_stack=lake_stack, base_stack=base_stack, prefix=environment, max_concurrent_runs=30
)

rfm_train_flow, rfm_transform_flow = deploy_algorithm_workflow(
    algo='rfm',
    prep_train_features=prepare_training_features_for_rfm_job,
    prep_transform_features=prepare_transform_features_for_rfm_job,
    output_to_pred=rfm_output_to_predictions_job,
    train_gen=algorithms.rfm.train_flow.generate_workflow_definition_lift,
    transform_gen=algorithms.rfm.transform_flow.generate_workflow_definition_lift
)

# nbo multitask


prepare_transform_features_for_nbom_job = cvm_glue_job(
    job_path="algorithms/nbom/prepare_features_for_transform.py",
    description="This job is responsible for preparing transform/inference features for NBOM",
    stage=Stage.Ingestion, lake_stack=lake_stack,
    base_stack=base_stack, prefix=environment, worker_type='G.2X',
)

prepare_training_features_for_nbom_job = cvm_glue_job(
    job_path="algorithms/nbom/prepare_features_for_training.py",
    description="This job is responsible for preparing training features for NBOM",
    stage=Stage.Ingestion, lake_stack=lake_stack,
    base_stack=base_stack, prefix=environment, worker_type='G.2X',
)

nbom_output_to_predictions_job = cvm_glue_job(
    job_path="algorithms/nbom/output_to_predictions_job.py",
    description="This job take flexible output for MBA and add data to the ProfileAlgorithmsOutput table",
    stage=Stage.Ingestion, lake_stack=lake_stack, base_stack=base_stack, prefix=environment
)

nbom_train_flow, nbom_transform_flow = deploy_algorithm_workflow(
    algo='nbom',
    prep_train_features=prepare_training_features_for_nbom_job,
    prep_transform_features=prepare_transform_features_for_nbom_job,
    output_to_pred=nbom_output_to_predictions_job,
    train_gen=algorithms.nbom.train_flow.generate_workflow_definition_lift,
    transform_gen=algorithms.nbom.transform_flow.generate_workflow_definition_lift
)

# -------------------------------LAMBDA---------------------------------------------

# runtime
# https://docs.aws.amazon.com/lambda/latest/dg/API_CreateFunction.html#SSS-CreateFunction-request-Runtime

lambda_sagemaker_get_latest_models = cvm_lambda_function(
    script_path="flexible_profile/sagemaker_get_latest_models.py", prefix=environment, runtime="python3.10",
    main_function_name="lambda_handler"

)

# -------------------------------WORKFLOWS-----------------------------------------

braze_audience_workflow = cvm_workflow(
    name=f"cmv_{environment}_braze_audience_workflow",
    stage=Stage.ApiExtract,
    definition_gen=workflow_lift(
        braze_audience_flow.generate_workflow_definition,
        audience_builder_job=audience_builder_job.name,
        braze_push_job=braze_push_audiences_job.name,
        audience_crawler=get_crawler_name(lake_descriptor, conformed.Audiences),
        audience_payload_crawler=get_crawler_name(lake_descriptor, conformed.AudiencePayload),
        audience_array_crawler=get_crawler_name(lake_descriptor, conformed.AudienceArray),
    )
)

braze_new_audience_workflow = cvm_workflow(
    name=f"cmv_{environment}_braze_new_audience_workflow",
    stage=Stage.ApiExtract,
    definition_gen=workflow_lift(
        braze_new_audience_flow.generate_workflow_definition,
        audience_builder_job=delta_build_audience.name,
        braze_push_job=braze_push_audiences_new_job.name,
        audience_crawler=get_crawler_name(lake_descriptor, conformed.Audiences),
        audience_braze_crawler=get_crawler_name(lake_descriptor, conformed.AudiencesBraze),
        audience_payload_crawler=get_crawler_name(lake_descriptor, conformed.AudiencePayload),
        delta_mapping_crawler=get_crawler_name(lake_descriptor, conformed.DeltaMapping),
        audience_array_crawler=get_crawler_name(lake_descriptor, conformed.AudienceArray),
        offer_braze_crawler=get_crawler_name(lake_descriptor, conformed.AudiencesOfferBraze),
        offer_share_crawler=get_crawler_name(lake_descriptor, conformed.AudiencesOfferShare),
    )
)

promotions_workflow = cvm_workflow(
    name=f"cmv_{environment}_promotions_workflow", stage=Stage.Ingestion,
    definition_gen=workflow_lift(
        promotions_workflow.generate_promotions_workflow_content,
        promotions_landing_crawler=get_crawler_name(lake_descriptor, landing.Promotions),
        promotions_staging_crawler=get_crawler_name(lake_descriptor, staging.Promotions),
        promotions_conformed_crawler=get_crawler_name(lake_descriptor, conformed.Promotions),
        ingestion_job=promotions_ingestion_job.name,
        merging_job=promotions_merging_job.name,
        offerbank_job=load_offers_to_bank_job.name,
    )
)

bu_external_id_workflow = cvm_workflow(
    name=f"cmv_{environment}_bu_external_id_workflow", stage=Stage.Ingestion,
    definition_gen=workflow_lift(
        bu_external_id_workflow.generate_bu_external_id_workflow_content,
        bu_external_id_landing_crawler=get_crawler_name(lake_descriptor, landing.BuExternalId),
        bu_external_id_staging_crawler=get_crawler_name(lake_descriptor, staging.BuExternalId),
        bu_external_id_conformed_crawler=get_crawler_name(lake_descriptor, conformed.BuExternalId),
        ingestion_job=bu_external_id_ingestion_job.name,
        merging_job=bu_external_id_merging_job.name,

    )
)

share_audiences_workflow = cvm_workflow(
    name=f"cmv_{environment}_share_audiences_workflow", stage=Stage.Ingestion,
    definition_gen=workflow_lift(
        share_audiences_workflow.generate_share_audiences_workflow_content,
        share_audiences_landing_crawler=get_crawler_name(lake_descriptor, landing.ShareAudience),
        share_audiences_staging_crawler=get_crawler_name(lake_descriptor, staging.ShareAudience),
        share_audiences_conformed_crawler=get_crawler_name(lake_descriptor, conformed.ShareAudience),
        ingestion_job=share_audience_ingestion.name,
        merging_job=share_audience_merging_job.name,

    )
)

build_customer_profiles_workflow = cvm_workflow(
    name=f"cmv_{environment}_customer_profiles_workflow", stage=Stage.Ingestion,
    definition_gen=workflow_lift(
        customer_profiles_workflow.generate_customer_profiles_workflow_content,
        c360_job=c360_profiles_creation_job_new.name,
        customer_profiles_conformed_crawler=get_crawler_name(lake_descriptor, conformed.CustomerProfile),
        stats_job=calculate_cdp_results_stat.name,
        results_job=load_cdp_results_job.name,
    )
)

refresh_profiles_workflow = cvm_workflow(
    name=f"cmv_{environment}_refresh_profiles_workflow", stage=Stage.Ingestion,
    definition_gen=workflow_lift(
        refresh_profiles_workflow.generate_refresh_profiles_workflow_content,
        refresh_job=refresh_audience_stat.name,
    )
)

ingestion_workflow = cvm_workflow(
    name=f"cmv_{environment}_ingestion_workflow",
    stage=Stage.Ingestion,
    definition_gen=workflow_lift(
        ingestion_flow.ingestion_workflow_definition,
        landing_crawlers=lake_stack.get_output("landing.crawlers"),
        staging_crawlers=lake_stack.get_output("staging.crawlers"),
        conformed_crawlers=lake_stack.get_output("conformed.crawlers"),
        prep_jobs=[
            # prep_demographics_job.name,
            # prep_id_mapping_job.name,
        ],
        landing_to_staging_jobs=[
            location_ingestion_job.name,
            products_ingestion_job.name,
            promotions_ingestion_job.name,
            id_mapping_ingestion_job.name,
            demographics_ingestion_job.name,
            loyalty_ingestion_ingestion_job.name,
            transactions_ingestion_job.name,
            custom_attributes_ingestion_job.name,
            share_audience_ingestion.name,

        ],
        staging_to_conformed_jobs=[
            location_merging_job.name,
            products_merging_job.name,
            promotions_merging_job.name,
            id_mappings_merging_job.name,
            demographics_merging_job.name,
            loyalty_merging_job.name,
            transactions_merging_job.name,
            custom_attributes_merging_job.name,
            share_audience_merging_job.name,

        ],
        post_ingestion_jobs=[
            # prep_algos_features_job.name
        ]
    ),
)

train_clv_models_for_customer_profiles = cvm_workflow(
    name=f"cmv_{environment}_clv_train_models_for_customer_profiles_workflow", stage=Stage.Ingestion,
    definition_gen=workflow_lift(
        train_clv_models_for_customer_profiles.generate_train_models_for_customer_profiles,
        train_flow_arn=clv_train_flow.arn,
        lake_descriptor=descriptor,
        kms_key_id=kms_key_id,
        environment=environment
    )
)

train_nbo_models_for_customer_profiles = cvm_workflow(
    name=f"cmv_{environment}_nbo_train_models_for_customer_profiles_workflow", stage=Stage.Ingestion,
    definition_gen=workflow_lift(
        train_nbo_models_for_customer_profiles.generate_train_models_for_customer_profiles,
        train_flow_arn=nbo_train_flow.arn,
        lake_descriptor=descriptor,
        kms_key_id=kms_key_id,
        environment=environment
    )
)

train_rfm_models_for_customer_profiles = cvm_workflow(
    name=f"cmv_{environment}_rfm_train_models_for_customer_profiles_workflow", stage=Stage.Ingestion,
    definition_gen=workflow_lift(
        train_rfm_models_for_customer_profiles.generate_train_models_for_customer_profiles,
        train_flow_arn=rfm_train_flow.arn,
        lake_descriptor=descriptor,
        kms_key_id=kms_key_id,
        environment=environment
    )
)

prepare_data_for_customer_profiles = cvm_workflow(
    name=f"cmv_{environment}_prepare_data_for_customer_profiles_workflow", stage=Stage.Ingestion,
    definition_gen=workflow_lift(
        prepare_data_for_customer_profiles.generate_prepare_data_for_customer_profiles,
        lambda_sagemaker_get_latest_models=lambda_sagemaker_get_latest_models.name,
        clv_transform_flow_arn=clv_transform_flow.arn,
        rfm_transform_flow_arn=rfm_transform_flow.arn,
        nbo_transform_flow_arn=nbo_transform_flow.arn,
        lake_descriptor=descriptor,
        kms_key_id=kms_key_id,
        environment=environment
    )
)

run_all = cvm_workflow(
    name=f"cmv_{environment}_run_all_workflow", stage=Stage.Ingestion,
    definition_gen=workflow_lift(
        run_all.generate_run_all,
        ingestion_flow_arn=ingestion_workflow.arn,
        clv_train_flow_arn=train_clv_models_for_customer_profiles.arn,
        rfm_train_flow_arn=train_rfm_models_for_customer_profiles.arn,
        calculated_kpi_job_name=calculated_kpi.name,
        prepare_data_for_customer_profiles_flow_arn=prepare_data_for_customer_profiles.arn,
        build_customer_profiles_workflow_flow_arn=build_customer_profiles_workflow.arn,
        lake_descriptor=descriptor,
        environment=environment
    )
)


# -------------------------------SCHEDULING-----------------------------------------
def enable_scheduler(current_env: str):
    return 'ENABLED' if current_env in ['prod', 'uat'] else 'DISABLED'


aws.scheduler.Schedule(f"cvm_{environment}_braze_new_audience_workflow_scheduler", aws.scheduler.ScheduleArgs(
    state=enable_scheduler(environment),
    group_name="default",
    schedule_expression="rate(1 hour)",
    flexible_time_window=aws.scheduler.ScheduleFlexibleTimeWindowArgs(
        mode="OFF"
    ),
    target=aws.scheduler.ScheduleTargetArgs(
        arn=braze_new_audience_workflow.arn,
        role_arn=braze_new_audience_workflow.role_arn
    )
))

aws.scheduler.Schedule(f"cvm_{environment}_braze_audience_workflow_scheduler", aws.scheduler.ScheduleArgs(
    state='DISABLED',
    group_name="default",
    schedule_expression="rate(2 hour)",
    flexible_time_window=aws.scheduler.ScheduleFlexibleTimeWindowArgs(
        mode="OFF"
    ),
    target=aws.scheduler.ScheduleTargetArgs(
        arn=braze_audience_workflow.arn,
        role_arn=braze_audience_workflow.role_arn
    )
))

aws.scheduler.Schedule(f"cvm_{environment}_promotions_workflow_scheduler", aws.scheduler.ScheduleArgs(
    state=enable_scheduler(environment),
    group_name="default",
    schedule_expression="cron(30 6 * * ? *)",
    flexible_time_window=aws.scheduler.ScheduleFlexibleTimeWindowArgs(
        mode="OFF"
    ),
    target=aws.scheduler.ScheduleTargetArgs(
        arn=promotions_workflow.arn,
        role_arn=promotions_workflow.role_arn
    )
))

aws.scheduler.Schedule(f"cvm_{environment}_customer_profiles_workflow_scheduler", aws.scheduler.ScheduleArgs(
    state='DISABLED',
    group_name="default",
    schedule_expression="cron(0 19 * * ? *)",
    flexible_time_window=aws.scheduler.ScheduleFlexibleTimeWindowArgs(
        mode="OFF"
    ),
    target=aws.scheduler.ScheduleTargetArgs(
        arn=build_customer_profiles_workflow.arn,
        role_arn=build_customer_profiles_workflow.role_arn
    )
))

aws.scheduler.Schedule(f"cmv_{environment}_bu_external_id_workflow_scheduler", aws.scheduler.ScheduleArgs(
    state=enable_scheduler(environment),
    group_name="default",
    schedule_expression="cron(30 6 * * ? *)",
    flexible_time_window=aws.scheduler.ScheduleFlexibleTimeWindowArgs(
        mode="OFF"
    ),
    target=aws.scheduler.ScheduleTargetArgs(
        arn=bu_external_id_workflow.arn,
        role_arn=bu_external_id_workflow.role_arn
    )
))

aws.scheduler.Schedule(f"cmv_{environment}_share_audiences_workflow_scheduler", aws.scheduler.ScheduleArgs(
    state=enable_scheduler(environment),
    group_name="default",
    schedule_expression="cron(30 6 * * ? *)",
    flexible_time_window=aws.scheduler.ScheduleFlexibleTimeWindowArgs(
        mode="OFF"
    ),
    target=aws.scheduler.ScheduleTargetArgs(
        arn=share_audiences_workflow.arn,
        role_arn=share_audiences_workflow.role_arn
    )
))

aws.scheduler.Schedule(f"cvm_{environment}_refresh_profiles_workflow_scheduler", aws.scheduler.ScheduleArgs(
    state=enable_scheduler(environment),
    group_name="default",
    schedule_expression="cron(30 2 * * ? *)",
    flexible_time_window=aws.scheduler.ScheduleFlexibleTimeWindowArgs(
        mode="OFF"
    ),
    target=aws.scheduler.ScheduleTargetArgs(
        arn=refresh_profiles_workflow.arn,
        role_arn=refresh_profiles_workflow.role_arn
    )
))

aws.scheduler.Schedule(f"cmv_{environment}_run_all_workflow_scheduler", aws.scheduler.ScheduleArgs(
    state=enable_scheduler(environment),
    group_name="default",
    schedule_expression="cron(0 12 ? * 1-5 *)",
    flexible_time_window=aws.scheduler.ScheduleFlexibleTimeWindowArgs(
        mode="OFF"
    ),
    target=aws.scheduler.ScheduleTargetArgs(
        arn=run_all.arn,
        role_arn=ingestion_workflow.role_arn
    )
))
