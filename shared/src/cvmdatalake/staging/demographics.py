from enum import Enum

from cvmdatalake import ColumnSpec, TableSpec


class Demographics(TableSpec):
    @classmethod
    def table_description(cls):
        return "Input Demographics data provided by MAF"

    bu = ColumnSpec(
        is_partition=True,
        data_type='string',
        description='Business Unit'
    )

    dat_snapshot = ColumnSpec(
        data_type='date',
        description='Snapshot date, the date on which the data was captured and exported from the System of Record (SOR) to the file'
    )

    idi_counterparty = ColumnSpec(
        data_type='string',
        description='Unique ID for counterparty in specific source system'
    )

    cod_sor_counterparty = ColumnSpec(
        data_type='string',
        description='Numeric ID of the System of Record (SOR) where your IDI_COUNTERPARTY is recorded'
    )

    cdi_counterparty_role = ColumnSpec(
        data_type='string',
        description='Code of role of counterparty'
    )

    des_counterparty_role = ColumnSpec(
        data_type='string',
        description='Description of role of counterparty'
    )

    cdi_counterparty_status = ColumnSpec(
        data_type='string',
        description='Code of status of counterparty: Example  A, NA, OH, etc'
    )

    des_counterparty_status = ColumnSpec(
        data_type='string',
        description='Dsecprition of status of counterparty: Example Given: Active, Not Active, On Hold, etc'
    )

    cde_country_residence = ColumnSpec(
        data_type='string',
        description='ISO-3166 (e.g. NL)'
    )

    nam_person_fullname = ColumnSpec(
        data_type='string',
        description='Name (first name prefix last name) - either use this field for the complete name or the following fields for parts of it'
    )

    nam_person_firstname = ColumnSpec(
        data_type='string',
        description='First name of the person'
    )

    nam_person_initials = ColumnSpec(
        data_type='string',
        description='Initials of the person'
    )

    nam_person_lastname = ColumnSpec(
        data_type='string',
        description='Last name of the person'
    )

    ind_person_gender = ColumnSpec(
        data_type='string',
        description='M = Male, F = Female, U = Unknown'
    )

    nam_person_titles = ColumnSpec(
        data_type='string',
        description='Titles of the person'
    )

    nam_person_profession = ColumnSpec(
        data_type='string',
        description='B2B: description of function of contact person'
    )

    ind_person_optin = ColumnSpec(
        data_type='string',
        description='Y = Yes, N = N, ? = unknown'
    )

    dat_person_optin = ColumnSpec(
        data_type='date',
        description='Date of registration of optin'
    )

    ide_person_telephonenumber = ColumnSpec(
        data_type='string',
        description='Personal mobile phone number. Please include country code'
    )

    ind_person_telephonenumber_optin = ColumnSpec(
        data_type='string',
        description='Y = Yes, N = N, ? = unknown'
    )

    dat_person_telephonenumber_optin = ColumnSpec(
        data_type='date',
        description='Date of registration of optin'
    )

    ide_person_telephonenumber_2 = ColumnSpec(
        data_type='string',
        description='Personal mobile phone number. Please include country code'
    )

    ind_person_telephonenumber_optin_2 = ColumnSpec(
        data_type='string',
        description='Y = Yes, N = N, ? = unknown'
    )

    dat_person_telephonenumber_optin_2 = ColumnSpec(
        data_type='date',
        description='Date of registration of optin 2'
    )

    ide_person_email = ColumnSpec(
        data_type='string',
        description='Primary email address of person'
    )

    ind_person_email_optin = ColumnSpec(
        data_type='string',
        description='Y = Yes, N = N, ? = unknown'
    )

    dat_person_email_optin = ColumnSpec(
        data_type='date',
        description='Date of registration of optin'
    )

    ide_person_sms = ColumnSpec(
        data_type='string',
        description='Personal mobile phone number used for sms messages. Please include country code'
    )

    ind_person_sms_optin = ColumnSpec(
        data_type='string',
        description='Y = Yes, N = N, ? = unknown'
    )

    dat_person_sms_optin = ColumnSpec(
        data_type='date',
        description='Date of registration of optin'
    )

    idi_owner = ColumnSpec(
        data_type='string',
        description='Unique ID for owner in specific source system'
    )

    cod_sor_owner = ColumnSpec(
        data_type='string',
        description='Numeric ID of the System of Record (SOR) where your IDI_OWNER is recorded'
    )

    idi_owner_2 = ColumnSpec(
        data_type='string',
        description='If appliacble a second Unique ID for owner in specific source system'
    )

    cod_sor_owner_2 = ColumnSpec(
        data_type='string',
        description='Numeric ID of the System of Record (SOR) where your IDI_OWNER is recorded'
    )

    adr_postal_cde_country = ColumnSpec(
        data_type='string',
        description='ISO-3166 (e.g. NL)'
    )

    adr_postal_address_string = ColumnSpec(
        data_type='string',
        description='Complete address (either use this field for the complete address or use the following fields for parts of it)'
    )

    adr_postal_streetname = ColumnSpec(
        data_type='string',
        description='Street name or PO-Box'
    )

    adr_postal_streetnumber = ColumnSpec(
        data_type='string',
        description='House number or number of PO-Box'
    )

    adr_postal_streetnumber_addition = ColumnSpec(
        data_type='string',
        description='House number addition or empty for PO-Box'
    )

    adr_postal_local_specific = ColumnSpec(
        data_type='string',
        description='Specific location, such as room number within a hotel'
    )

    adr_postal_postalcode = ColumnSpec(
        data_type='string',
        description='Postal code or postal code of PO-Box'
    )

    adr_postal_cityname = ColumnSpec(
        data_type='string',
        description='City name or city name of PO-Box'
    )

    adr_visit_cde_country = ColumnSpec(
        data_type='string',
        description='ISO-3166 (e.g. NL)'
    )

    adr_visit_address_string = ColumnSpec(
        data_type='string',
        description='Complete address (either use this field for the complete address or use the following fields for parts of it)'
    )

    adr_visit_streetname = ColumnSpec(
        data_type='string',
        description='Street name of visit location'
    )

    adr_visit_streetnumber = ColumnSpec(
        data_type='string',
        description='House number of visit location'
    )

    adr_visit_streetnumber_addition = ColumnSpec(
        data_type='string',
        description='House number addition or visit location'
    )

    adr_visit_local_specific = ColumnSpec(
        data_type='string',
        description='Specific location of visit location, such as room number within a hotel'
    )

    adr_visit_postalcode = ColumnSpec(
        data_type='string',
        description='Postal code or postal code of visit location'
    )

    adr_visit_cityname = ColumnSpec(
        data_type='string',
        description='City name or city name of visit location'
    )

    dat_of_birth = ColumnSpec(
        data_type='date',
        description='Date of birth of person'
    )

    des_age = ColumnSpec(
        data_type='bigint',
        description='Age of person at dat_snapshot'
    )

    cde_language = ColumnSpec(
        data_type='string',
        description='Language code (ISO)'
    )

    des_language = ColumnSpec(
        data_type='string',
        description='Description of language (English, Arabic, Dutch)'
    )

    des_type_optin_email = ColumnSpec(
        data_type='string',
        description='Description of type(s) of optins granted by this user'
    )

    des_age_group = ColumnSpec(
        data_type='string',
        description=''
    )

    dat_of_joining_loyalty = ColumnSpec(
        data_type='string',
        description=''
    )

    dat_first_login = ColumnSpec(
        data_type='string',
        description=''
    )

    des_enrollment_channel = ColumnSpec(
        data_type='string',
        description=''
    )

    cde_enrollment_bu_key = ColumnSpec(
        data_type='string',
        description=''
    )

    dat_first_earn = ColumnSpec(
        data_type='string',
        description=''
    )

    dat_last_earn = ColumnSpec(
        data_type='string',
        description=''
    )

    dat_first_redemption = ColumnSpec(
        data_type='string',
        description=''
    )

    dat_last_redemption = ColumnSpec(
        data_type='string',
        description=''
    )

    cua_current_balance_basic = ColumnSpec(
        data_type='string',
        description=''
    )