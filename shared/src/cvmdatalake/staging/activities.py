from enum import Enum

from cvmdatalake import TableSpec, ColumnSpec


class Activities(TableSpec):

    @classmethod
    def table_description(cls):
        return "Input Activities data provided by MAF"

    # bu = ColumnSpec(
    #     is_partition=True,
    #     data_type='string',
    #     description='Business Unit'
    # )
    
    dat_activity = ColumnSpec(
        is_partition=True,
        data_type='string',
        description='Date of activity'
    )

    dat_batch = ColumnSpec(
        data_type='date',
        description='Batch, the date on which the data was captured and exported from the System of Record (SOR) to the file. The Batch date is the same for every record in the File. '
    )

    idi_activity = ColumnSpec(
        data_type='string',
        description='Unique ID of Activity in specific source system'
    )

    cod_sor_activity = ColumnSpec(
        data_type='string',
        description='Numeric ID of the System of Record (SOR) where your IDI_ACTIVITY is recorded'
    )

    ind_counterparty_type = ColumnSpec(
        data_type='string',
        description='An activity can refer to a business customer (organization) or to a consumer customer (Person), but both are counterparties.'
    )

    idi_counterparty = ColumnSpec(
        data_type='string',
        description='ID of counterparty (org or person) of activity'
    )

    cod_sor_counterparty = ColumnSpec(
        data_type='string',
        description='Numeric ID of the System of Record (SOR) where your IDI_COUNTERPARTY is recorded'
    )

    idi_counterparty_gr = ColumnSpec(
        data_type='string',
        description='GR ID of counterparty (org or person) of activity'
    )

    cod_sor_counterparty_gr = ColumnSpec(
        data_type='string',
        description='Numeric GR ID of the System of Record (SOR)'
    )

    idi_owner = ColumnSpec(
        data_type='string',
        description='ID of activity owner'
    )

    cod_sor_owner = ColumnSpec(
        data_type='string',
        description='Numeric ID of the System of Record (SOR) where your IDI_ACTIVITY is recorded'
    )

    idi_owner_2 = ColumnSpec(
        data_type='string',
        description='ID of activity owner 02. Please use 9999 if owner is unknown'
    )

    cod_sor_owner_2 = ColumnSpec(
        data_type='string',
        description='Numeric ID of the System of Record (SOR) where your IDI_OWNER_2 is recorded'
    )

    idi_proposition = ColumnSpec(
        data_type='string',
        description='If applicable: ID of proposition'
    )

    cod_sor_proposition = ColumnSpec(
        data_type='string',
        description='Numeric ID of the System of Record (SOR) where your IDI_PROPOSITION is recorded'
    )

    cdi_activity_status = ColumnSpec(
        data_type='string',
        description='Code for Status of activity (CO,PE,CA, ...)'
    )

    des_activity_status = ColumnSpec(
        data_type='string',
        description='Description for Status of activity (completed, pending, cancelled,  ...)'
    )

    cdi_activity_type = ColumnSpec(
        data_type='string',
        description='Code for Type of activity (e.g. V,C,E, ...)'
    )

    des_activity_type = ColumnSpec(
        data_type='string',
        description='Description for Type of activity (e.g. visit, call, e-mail,...)'
    )

    cdi_activity_reason_no_deal = ColumnSpec(
        data_type='string',
        description='If cancelled the Code for the reason of Cancellation (e.g.: LD, WI, PR...)'
    )

    des_activity_reason_no_deal = ColumnSpec(
        data_type='string',
        description='If cancelled the Description for the reason of Cancellation (e.g. Lost Deal, Withdrawn, Pricing, ...)'
    )

    cdi_activity_channel = ColumnSpec(
        data_type='string',
        description='Code for the channel of activity (e.g.: S, M, D,...)'
    )

    des_activity_channel = ColumnSpec(
        data_type='string',
        description='Description for the channel of activity (e.g.: Sales, Marketing, Digital ,...)'
    )

    tim_activity = ColumnSpec(
        data_type='string',
        description='DateTime of activity (start, or when the event happened'
    )

    utc_activity = ColumnSpec(
        data_type='string',
        description='Universal Date Time of activity, optional when organization works international in different time zones'
    )

    dat_activity_follow_up = ColumnSpec(
        data_type='date',
        description='Data when follow-up activity has been scheduled'
    )

    dat_activity_last_mutation = ColumnSpec(
        data_type='date',
        description='Date when the activity has last been updated in source system'
    )

    idi_campaign = ColumnSpec(
        data_type='string',
        description='idi_campaign value'
    )

    des_campaign_name = ColumnSpec(
        data_type='string',
        description='des_campaign_name value'
    )

    idi_user_id = ColumnSpec(
        data_type='string',
        description='idi_user_id value'
    )

    idi_external_user_id = ColumnSpec(
        data_type='string',
        description='idi_external_user_id value'
    )

    cde_dispatch_id = ColumnSpec(
        data_type='string',
        description='cde_dispatch_id value'
    )

    des_email_address = ColumnSpec(
        data_type='string',
        description='des_email_address value'
    )

    cde_app_id = ColumnSpec(
        data_type='string',
        description='cde_app_id value'
    )

    cde_device_id = ColumnSpec(
        data_type='string',
        description='cde_device_id value'
    )

    des_marketing_area = ColumnSpec(
        data_type='string',
        description='des_marketing_area value'
    )

    des_message_action = ColumnSpec(
        data_type='string',
        description='des_message_action value'
    )

    des_message_event = ColumnSpec(
        data_type='string',
        description='des_message_event value'
    )

    dat_campaign_start_date = ColumnSpec(
        data_type='date',
        description='dat_campaign_start_date value'
    )

    dat_campaign_end_date = ColumnSpec(
        data_type='date',
        description='dat_campaign_end_date value'
    )

    des_campaign_category = ColumnSpec(
        data_type='string',
        description='des_campaign_category value'
    ) 

    ind_marked_spams = ColumnSpec(
        data_type='string',
        description='ind_marked_spams value'
    )

    dat_campaign_creation_date = ColumnSpec(
        data_type='date',
        description='dat_campaign_creation_date value'
    )

    dat_processing_date = ColumnSpec(
        data_type='date',
        description='dat_processing_date value'
    )

    des_impression = ColumnSpec(
	    data_type='string',
	    description='des_impression value'
    ) 

    idi_ga_session_id = ColumnSpec(
        data_type='string',
        description='idi_ga_session_id value'
    ) 

    idi_ga_seesion_number = ColumnSpec(
        data_type='string',
        description='idi_ga_seesion_number value'
    ) 

    des_page_title = ColumnSpec(
        data_type='string',
        description='des_page_title value'
    )

    tim_engagemnt_time_msec = ColumnSpec(
        data_type='string',
        description='tim_engagemnt_time_msec value'
    ) 

    des_page_location = ColumnSpec(
        data_type='string',
        description='des_page_location value'
    )

    des_link_url = ColumnSpec(
        data_type='string',
        description='des_link_url value'
    )

    idi_user_pseudo_id = ColumnSpec(
        data_type='string',
        description='idi_user_pseudo_id value'
    ) 

    des_privacy_info_uses_transient_token = ColumnSpec(
        data_type='string',
        description='des_privacy_info_uses_transient_token value'
    )

    des_device_category = ColumnSpec(
        data_type='string',
        description='des_device_category value'
    ) 

    des_device_mobile_brand_name = ColumnSpec(
        data_type='string',
        description='des_device_mobile_brand_name value'
    ) 

    des_device_mobile_model_name = ColumnSpec(
        data_type='string',
        description='des_device_mobile_model_name value'
    ) 

    des_device_operating_system = ColumnSpec(
        data_type='string',
        description='des_device_operating_system value'
    ) 

    des_device_operating_system_version = ColumnSpec(
        data_type='string',
        description='des_device_operating_system_version value'
    ) 

    des_device_language = ColumnSpec(
        data_type='string',
        description='des_device_language value'
    ) 

    des_device_is_limited_ad_tracking = ColumnSpec(
        data_type='string',
        description='des_device_is_limited_ad_tracking value'
    ) 

    des_device_web_info = ColumnSpec(
        data_type='string',
        description='des_device_web_info value'
    ) 

    des_device_browser_version = ColumnSpec(
        data_type='string',
        description='des_device_browser_version value'
    ) 

    des_device_web_info_hostname = ColumnSpec(
        data_type='string',
        description='des_device_web_info_hostname value'
    ) 

    des_geo_continent = ColumnSpec(
        data_type='string',
        description='des_geo_continent value'
    ) 

    des_geo_county = ColumnSpec(
        data_type='string',
        description='des_geo_county value'
    ) 

    des_geo_region = ColumnSpec(
        data_type='string',
        description='des_geo_region value'
    ) 

    des_geo_city = ColumnSpec(
        data_type='string',
        description='des_geo_city value'
    ) 

    des_geo_sub_continent = ColumnSpec(
        data_type='string',
        description='des_geo_sub_continent value'
    ) 

    des_traffic_source_name = ColumnSpec(
        data_type='string',
        description='des_traffic_source_name value'
    ) 

    des_traffic_source_medium = ColumnSpec(
        data_type='string',
        description='des_traffic_source_medium value'
    ) 

    des_traffic_source_source = ColumnSpec(
        data_type='string',
        description='des_traffic_source_source value'
    ) 

    idi_stream_id = ColumnSpec(
        data_type='string',
        description='idi_stream_id value'
    ) 
    
    des_platform = ColumnSpec(
        data_type='string',
        description='des_platform value'
    )