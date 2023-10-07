from cvmdatalake import TableSpec, ColumnSpec


class Audiences(TableSpec):

    @classmethod
    def table_description(cls):
        return "Table contains NBO output"

    external_id = ColumnSpec(
        data_type='string',
        description='Braze ID'
    )

    gcr_id = ColumnSpec(
        data_type='string',
        description=''
    )

    share_id = ColumnSpec(
        data_type='string',
        description=''
    )

    audience = ColumnSpec(
        data_type='string',
        description='Audience Name'
    )

    business_unit = ColumnSpec(
        data_type='string',
        description=''
    )

    campaign_start_date = ColumnSpec(
        data_type='date',
        description=''
    )

    campaign_end_date = ColumnSpec(
        data_type='date',
        description=''
    )

    campaign_type = ColumnSpec(
        data_type='string',
        description=''
    )

    active_appgroup = ColumnSpec(
        data_type='boolean',
        description=''
    )

    ingestion_timestamp = ColumnSpec(
        data_type='timestamp',
        description=''
    )


class AudiencesBraze(TableSpec):

    @classmethod
    def table_description(cls):
        return "Table contains append of audiences table for sending array to braze output"

    external_id = ColumnSpec(
        data_type='string',
        description='Braze ID'
    )

    gcr_id = ColumnSpec(
        data_type='string',
        description=''
    )

    share_id = ColumnSpec(
        data_type='string',
        description='share_id'
    )

    audience = ColumnSpec(
        data_type='string',
        description='Audience Name'
    )

    business_unit = ColumnSpec(
        data_type='string',
        description=''
    )

    campaign_start_date = ColumnSpec(
        data_type='date',
        description='date'
    )

    campaign_end_date = ColumnSpec(
        data_type='date',
        description='date'
    )

    campaign_type = ColumnSpec(
        data_type='string',
        description='string'
    )

    active_appgroup = ColumnSpec(
        data_type='boolean',
        description=''
    )

    ingestion_timestamp = ColumnSpec(
        data_type='timestamp',
        description=''
    )

class AudiencesBrazeUCG(TableSpec):

    @classmethod
    def table_description(cls):
        return "Table contains append of audiences table for sending array to braze output"

    gcr_id = ColumnSpec(
        data_type='string',
        description=''
    )

    audience = ColumnSpec(
        data_type='string',
        description='Audience Name'
    )

    idi_measurement = ColumnSpec(
        data_type='string',
        description=''
    )

class DeltaMapping(TableSpec):

    @classmethod
    def table_description(cls):
        return "Table contains append of audiences which needs to be triggered again"

    gcr_id = ColumnSpec(
        data_type='string',
        description=''
    )

    external_id = ColumnSpec(
        data_type='string',
        description='Braze ID'
    )

    audience = ColumnSpec(
        data_type='string',
        description='Audience Name'
    )

    business_unit = ColumnSpec(
        data_type='string',
        description=''
    )

    campaign_end_date = ColumnSpec(
        data_type='date',
        description=''
    )

    ingestion_timestamp = ColumnSpec(
        data_type='timestamp',
        description=''
    )

class AudiencesOfferBraze(TableSpec):

    @classmethod
    def table_description(cls):
        return "Table contains offers overwritten by Offers assigned to Audiences/Campaigns in the UI for BRAZE customers"

    braze_id = ColumnSpec(
        data_type='string',
        description='Braze ID'
    )

    cvm_campaign_name = ColumnSpec(
        data_type='string',
        description='Audience Name'
    )

    idi_offer = ColumnSpec(
        data_type='array<string>',
        description='Offers to overwrite current offers served via Unified GraphQL API'
    )


class AudiencesOfferShare(TableSpec):

    @classmethod
    def table_description(cls):
        return "Table contains offers overwritten by Offers assigned to Audiences/Campaigns in the UI for SHARE customers"

    share_id = ColumnSpec(
        data_type='string',
        description='Braze ID'
    )

    cvm_campaign_name = ColumnSpec(
        data_type='string',
        description='Audience Name'
    )

    idi_offer = ColumnSpec(
        data_type='array<string>',
        description='Offers to overwrite current offers served via Unified GraphQL API'
    )


class AudiencePayload(TableSpec):
    @classmethod
    def table_description(cls):
        return "Table contains NBO output"

    payload = ColumnSpec(
        data_type='string',
        description='payload posted to MAF Braze Kafka endpoint'
    )

    result = ColumnSpec(
        data_type='string',
        description='HTTP response status'
    )


class AudienceArray(TableSpec):
    @classmethod
    def table_description(cls):
        return "Table contains NBO output"

    external_id = ColumnSpec(
        data_type='string',
        description='Braze ID'
    )

    business_unit = ColumnSpec(
        data_type='string',
        description=''
    )

    cvm_audience = ColumnSpec(
        data_type='array<string>',
        description='payload posted to MAF Braze Kafka endpoint'
    )

    ingestion_timestamp = ColumnSpec(
        data_type='timestamp',
        description=''
    )


class AudienceCmtdata(TableSpec):
    @classmethod
    def table_description(cls):
        return "Table contains NBO output"

    cvm_audience = ColumnSpec(
        data_type='string',
        description=''
    )

    idi_counterparty = ColumnSpec(
        data_type='string',
        description=''
    )

    campaign_name = ColumnSpec(
        data_type='string',
        description=''
    )

    ingestion_timestamp = ColumnSpec(
        data_type='timestamp',
        description=''
    )
