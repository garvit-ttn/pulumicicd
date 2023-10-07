from enum import Enum

from cvmdatalake import TableSpec, ColumnSpec


class ProfileAlgorithmsOutput(TableSpec):

    @classmethod
    def table_description(cls):
        return "Table for output of profile algorithms"

    data_filter_context = ColumnSpec(
        is_partition=True,
        data_type='string',
        description='Name from FlexibleProfileManager'
    )

    idi_counterparty = ColumnSpec(
        data_type='string',
        description='The grc_id of the counterparty involved in the transaction',
        customer_profile_name='IDI_COUNTERPARTY'
    )

    date_of_prepare = ColumnSpec(
        data_type='date',
        description='Date of preparation last input data from algorithm'
    )

    clv_frequency = ColumnSpec(
        data_type='string',
        description='The number of transactions made by the counterparty in the last 6 months',
        customer_profile_name='FREQUENCY'
    )

    clv_recency = ColumnSpec(
        data_type='string',
        description='The number of days since the last transaction made by the counterparty',
        customer_profile_name='RECENCY'
    )

    clv_t = ColumnSpec(
        data_type='string',
        description='The total number of days since the first transaction made by the counterparty',
        customer_profile_name='T'
    )

    clv_monetary = ColumnSpec(
        data_type='string',
        description='The average amount of money spent by the counterparty per transaction',
        customer_profile_name='RECENCY'
    )

    clv_6_months_expected_purchases = ColumnSpec(
        data_type='string',
        description='The predicted number of transactions that the counterparty will make in the next 6 months based on a probabilistic model',
        customer_profile_name='1_MONTHS_EXPECTED_PURCHASES'
    )

    clv_6_months_clv = ColumnSpec(
        data_type='string',
        description='The estimated value of the counterparty for the next 6 months based on their expected purchases and monetary value',
        customer_profile_name='1_MONTHS_CLV'
    )

    clv_6_prob_alive = ColumnSpec(
        data_type='string',
        description='The probability that the counterparty is still active and will make future transactions based on their recency and frequency',
        customer_profile_name='PROB_ALIVE'
    )

    nbo_json = ColumnSpec(
        data_type='string',
        description='Json with NBOs',
        customer_profile_name='NBO'
    )

    nbo_rank = ColumnSpec(
        data_type='string',
        description='Rank of recommendation NBOs',
        customer_profile_name='NBO'
    )

    rfm_idi_proposition_level03 = ColumnSpec(
        data_type='string',
        description='IDI proposition level for the customer1'
    )

    rfm_recency = ColumnSpec(
        data_type='string',
        description='Number of days since the last purchase of the customer'
    )

    rfm_frequency = ColumnSpec(
        data_type='int',
        description='Number of orders of the customer'
    )

    rfm_monetary = ColumnSpec(
        data_type='int',
        description='Value of orders of the customer'
    )

    rfm_r_rank_norm = ColumnSpec(
        data_type='int',
        description='Recency class (1-5)'
    )

    rfm_f_rank_norm = ColumnSpec(
        data_type='int',
        description='Frequency class (1-5)'
    )

    rfm_m_rank_norm = ColumnSpec(
        data_type='int',
        description='Monetary class (1-5)'
    )

    rfm_customer_segment = ColumnSpec(
        data_type='string',
        description='Customer segment based on RFM'
    )

    mba_idi_transaction = ColumnSpec(
        data_type='string',
        description=''
    )

    mba_items = ColumnSpec(
        data_type='string',
        description=''
    )

    mba_antecedents = ColumnSpec(
        data_type='string',
        description=''
    )

    mba_consequents = ColumnSpec(
        data_type='string',
        description=''
    )

    mba_antecedent_support = ColumnSpec(
        data_type='string',
        description=''
    )

    mba_consequent_support = ColumnSpec(
        data_type='string',
        description=''
    )

    mba_support = ColumnSpec(
        data_type='string',
        description=''
    )

    mba_confidence = ColumnSpec(
        data_type='string',
        description=''
    )

    mba_lift = ColumnSpec(
        data_type='string',
        description=''
    )

    mba_leverage = ColumnSpec(
        data_type='string',
        description=''
    )

    mba_conviction = ColumnSpec(
        data_type='string',
        description=''
    )
