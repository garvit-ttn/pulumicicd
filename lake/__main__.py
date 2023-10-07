import pulumi

import budgets

from common import cvm_crawler_role
from cvmdatalake import landing, staging, conformed, features

from conformed.zone import CvmConformedZone
from features.zone import CvmFeaturesZone
from landing.zone import CvmLandingZone
from staging.zone import CvmStagingZone

from cvmdatalake.pulumi import *

enable_debug(flag=False)

config = pulumi.Config()
environment = config.require("environment")

budgets.provision(environment)

crawler_role = cvm_crawler_role(environment)

# Landing Zone tables
landing_zone = CvmLandingZone(crawler_role, prefix=environment, bucket_name=config.get('landing_bucket_name'))

landing_zone.cvm_landing_table_from_spec(landing.Transactions)
landing_zone.cvm_landing_table_from_spec(landing.Activities)
landing_zone.cvm_landing_table_from_spec(landing.Promotions)
landing_zone.cvm_landing_table_from_spec(landing.Demographics)
landing_zone.cvm_landing_table_from_spec(landing.Products)
landing_zone.cvm_landing_table_from_spec(landing.IdMapping)
landing_zone.cvm_landing_table_from_spec(landing.AudienceInfo)
landing_zone.cvm_landing_table_from_spec(landing.Campaign)
landing_zone.cvm_landing_table_from_spec(landing.Offer)
landing_zone.cvm_landing_table_from_spec(landing.CustomAttributes)
landing_zone.cvm_landing_table_from_spec(landing.BuExternalId)
landing_zone.cvm_landing_table_from_spec(landing.Location)
landing_zone.cvm_landing_table_from_spec(landing.LoyaltyBusinessUnit)
landing_zone.cvm_landing_table_from_spec(landing.LoyaltyCountry)
landing_zone.cvm_landing_table_from_spec(landing.LoyaltyOpco)
landing_zone.cvm_landing_table_from_spec(landing.LoyaltySponsor)
landing_zone.cvm_landing_table_from_spec(landing.PtaSta)
landing_zone.cvm_landing_table_from_spec(landing.ShareAudience)

pulumi.export("landing.tables", landing_zone.get_tables_names())
pulumi.export("landing.crawlers", landing_zone.get_crawlers_names())

# Staging Zone tables  
staging_zone = CvmStagingZone(crawler_role, prefix=environment, bucket_name=config.get('staging_bucket_name'))
staging_zone.cvm_staging_table_from_spec(staging.Transactions)
staging_zone.cvm_staging_table_from_spec(staging.Activities)
staging_zone.cvm_staging_table_from_spec(staging.Promotions)
staging_zone.cvm_staging_table_from_spec(staging.Demographics)
staging_zone.cvm_staging_table_from_spec(staging.Products)
staging_zone.cvm_staging_table_from_spec(staging.IdMapping)
staging_zone.cvm_staging_table_from_spec(staging.CustomAttributes)
staging_zone.cvm_staging_table_from_spec(staging.Location)
staging_zone.cvm_staging_table_from_spec(staging.BuExternalId)
staging_zone.cvm_staging_table_from_spec(staging.LoyaltyBusinessUnit)
staging_zone.cvm_staging_table_from_spec(staging.LoyaltyCountry)
staging_zone.cvm_staging_table_from_spec(staging.LoyaltyOpco)
staging_zone.cvm_staging_table_from_spec(staging.LoyaltySponsor)
staging_zone.cvm_staging_table_from_spec(staging.PtaSta)
staging_zone.cvm_staging_table_from_spec(staging.ShareAudience)

pulumi.export("staging.tables", staging_zone.get_tables_names())
pulumi.export("staging.crawlers", staging_zone.get_crawlers_names())

# Conformed Zone tables
conformed_zone = CvmConformedZone(crawler_role, prefix=environment, bucket_name=config.get('conformed_bucket_name'))
conformed_zone.cvm_conformed_table_from_spec(conformed.Transactions)
conformed_zone.cvm_conformed_table_from_spec(conformed.Activities)
conformed_zone.cvm_conformed_table_from_spec(conformed.Promotions)
conformed_zone.cvm_conformed_table_from_spec(conformed.Demographics)
conformed_zone.cvm_conformed_table_from_spec(conformed.Products)
conformed_zone.cvm_conformed_table_from_spec(conformed.IdMapping)
conformed_zone.cvm_conformed_table_from_spec(conformed.CustomerProfile)
conformed_zone.cvm_conformed_table_from_spec(conformed.Predictions)
conformed_zone.cvm_conformed_table_from_spec(conformed.Offers)
conformed_zone.cvm_conformed_table_from_spec(conformed.Audiences)
conformed_zone.cvm_conformed_table_from_spec(conformed.CustomAttributes)
conformed_zone.cvm_conformed_table_from_spec(conformed.ProfileCalculatedKpi)
conformed_zone.cvm_conformed_table_from_spec(conformed.ProfileAlgorithmsOutput)
conformed_zone.cvm_conformed_table_from_spec(conformed.Location)
conformed_zone.cvm_conformed_table_from_spec(conformed.DataValidationDashboard)
conformed_zone.cvm_conformed_table_from_spec(conformed.AudiencePayload)
conformed_zone.cvm_conformed_table_from_spec(conformed.DeltaMapping)
conformed_zone.cvm_conformed_table_from_spec(conformed.AudienceArray)
conformed_zone.cvm_conformed_table_from_spec(conformed.AudiencesBraze)
conformed_zone.cvm_conformed_table_from_spec(conformed.AudiencesBrazeUCG)
conformed_zone.cvm_conformed_table_from_spec(conformed.AudienceCmtdata)
conformed_zone.cvm_conformed_table_from_spec(conformed.AudiencesOfferBraze)
conformed_zone.cvm_conformed_table_from_spec(conformed.AudiencesOfferShare)
conformed_zone.cvm_conformed_table_from_spec(conformed.BuExternalId)
conformed_zone.cvm_conformed_table_from_spec(conformed.ProfileMeasurements)
conformed_zone.cvm_conformed_table_from_spec(conformed.LoyaltyBusinessUnit)
conformed_zone.cvm_conformed_table_from_spec(conformed.LoyaltyCountry)
conformed_zone.cvm_conformed_table_from_spec(conformed.LoyaltyOpco)
conformed_zone.cvm_conformed_table_from_spec(conformed.LoyaltySponsor)
conformed_zone.cvm_conformed_table_from_spec(conformed.PtaSta)
conformed_zone.cvm_conformed_table_from_spec(conformed.ShareAudience)

pulumi.export("conformed.tables", conformed_zone.get_tables_names())
pulumi.export("conformed.crawlers", conformed_zone.get_crawlers_names())

features_zone = CvmFeaturesZone(prefix=environment, conformed_bucket_id=conformed_zone.conformed_bucket_id)

features_zone.cvm_feature_table_from_spec(features.RfmOutput)
features_zone.cvm_feature_table_from_spec(features.RfmModel)
features_zone.cvm_feature_table_from_spec(features.RfmTrainingInput)
features_zone.cvm_feature_table_from_spec(features.RfmTransformInput)

features_zone.cvm_feature_table_from_spec(features.ClvOutput)
features_zone.cvm_feature_table_from_spec(features.ClvModel)
features_zone.cvm_feature_table_from_spec(features.ClvTrainingInput)
features_zone.cvm_feature_table_from_spec(features.ClvTransformInput)

features_zone.cvm_feature_table_from_spec(features.NboOutput)
features_zone.cvm_feature_table_from_spec(features.NboModel)
features_zone.cvm_feature_table_from_spec(features.NboTrainingInput)
features_zone.cvm_feature_table_from_spec(features.NboTransformInput)

features_zone.cvm_feature_table_from_spec(features.NbomOutput)
features_zone.cvm_feature_table_from_spec(features.NbomModel)
features_zone.cvm_feature_table_from_spec(features.NbomTrainingInput)
features_zone.cvm_feature_table_from_spec(features.NbomTransformInput)

features_zone.cvm_feature_table_from_spec(features.SatOutput)
features_zone.cvm_feature_table_from_spec(features.SatModel)
features_zone.cvm_feature_table_from_spec(features.SatTrainingInput)
features_zone.cvm_feature_table_from_spec(features.SatTransformInput)

features_zone.cvm_feature_table_from_spec(features.MbaOutput)
features_zone.cvm_feature_table_from_spec(features.MbaModel)
features_zone.cvm_feature_table_from_spec(features.MbaTrainingInput)
features_zone.cvm_feature_table_from_spec(features.MbaTransformInput)

# required for migration services setup in jobs project
pulumi.export("landing.bucket_id", landing_zone.landing_bucket.id)

pulumi.export(
    "lake_descriptor",
    merge_descriptors(
        landing_zone.get_zone_descriptor(),
        staging_zone.get_zone_descriptor(),
        conformed_zone.get_zone_descriptor(),
        features_zone.get_zone_descriptor(),
    ).to_pulumi_compatible_representation()
)
