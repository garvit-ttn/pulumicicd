from enum import Enum

from cvmdatalake import TableSpec, ColumnSpec


class Transactions(TableSpec):

    @classmethod
    def table_description(cls):
        return "Input Transactions data provided by MAF"

    bu = ColumnSpec(
        is_partition=True,
        data_type='string',
        description='Business Unit',
    )

    dat_date_type_1 = ColumnSpec(
        is_partition=True,
        data_type='date',
        description=''
    )

    dat_batch = ColumnSpec(
        data_type='date',
        description=''
    )

    dat_period_start = ColumnSpec(
        data_type='date',
        description=''
    )

    dat_period_end = ColumnSpec(
        data_type='date',
        description=''
    )

    idi_turnover = ColumnSpec(
        data_type='string',
        description=''
    )

    cod_sor_turnover = ColumnSpec(
        data_type='string',
        description=''
    )

    idi_counterparty_gr = ColumnSpec(
        data_type='string',
        description=''
    )

    cod_sor_gr = ColumnSpec(
        data_type='string',
        description=''
    )

    cdi_period_date_type = ColumnSpec(
        data_type='string',
        description=''
    )

    ind_counterparty_type = ColumnSpec(
        data_type='string',
        description=''
    )

    idi_counterparty = ColumnSpec(
        data_type='string',
        description=''
    )

    cod_sor_counterparty = ColumnSpec(
        data_type='string',
        description=''
    )

    idi_owner = ColumnSpec(
        data_type='string',
        description=''
    )

    cod_sor_owner = ColumnSpec(
        data_type='string',
        description=''
    )

    idi_owner_2 = ColumnSpec(
        data_type='string',
        description=''
    )

    cod_sor_owner_2 = ColumnSpec(
        data_type='string',
        description=''
    )

    idi_proposition = ColumnSpec(
        data_type='string',
        description=''
    )

    idi_proposition_level01 = ColumnSpec(
        data_type='string',
        description=''
    )

    des_cityname = ColumnSpec(
        data_type='string',
        description=''
    )

    cde_date_key = ColumnSpec(
        data_type='string',
        description=''
    )

    cde_store_key = ColumnSpec(
        data_type='string',
        description=''
    )

    cde_country_key = ColumnSpec(
        data_type='string',
        description=''
    )

    des_country_name = ColumnSpec(
        data_type='string',
        description=''
    )

    des_store_name = ColumnSpec(
        data_type='string',
        description=''
    )

    cdi_department = ColumnSpec(
        data_type='string',
        description=''
    )

    des_maf_merchant_name = ColumnSpec(
        data_type='string',
        description=''
    )

    cde_terminal_id = ColumnSpec(
        data_type='string',
        description=''
    )

    cde_crm_key = ColumnSpec(
        data_type='string',
        description=''
    )

    des_opco = ColumnSpec(
        data_type='string',
        description=''
    )

    des_bu = ColumnSpec(
        data_type='string',
        description=''
    )

    des_mall_name = ColumnSpec(
        data_type='string',
        description=''
    )

    ind_mall_flag = ColumnSpec(
        data_type='string',
        description=''
    )

    des_maf_brand_name = ColumnSpec(
        data_type='string',
        description=''
    )

    des_maf_category_group = ColumnSpec(
        data_type='string',
        description=''
    )

    des_maf_subcategory = ColumnSpec(
        data_type='string',
        description=''
    )

    des_merchant_category_group = ColumnSpec(
        data_type='string',
        description=''
    )

    des_merchant_subcategory = ColumnSpec(
        data_type='string',
        description=''
    )

    des_maf_category = ColumnSpec(
        data_type='string',
        description=''
    )

    des_relevant_categories = ColumnSpec(
        data_type='string',
        description=''
    )

    des_location = ColumnSpec(
        data_type='string',
        description=''
    )

    des_map_location = ColumnSpec(
        data_type='string',
        description=''
    )

    ind_is_gcr = ColumnSpec(
        data_type='string',
        description=''
    )

    ind_is_crf = ColumnSpec(
        data_type='string',
        description=''
    )

    ind_is_vox = ColumnSpec(
        data_type='string',
        description=''
    )

    des_ofline_online = ColumnSpec(
        data_type='string',
        description=''
    )

    cde_trx_key = ColumnSpec(
        data_type='string',
        description=''
    )

    qty_item_net_weight = ColumnSpec(
        data_type='string',
        description=''
    )

    cua_sell_price = ColumnSpec(
        data_type='string',
        description=''
    )

    cde_visit_key = ColumnSpec(
        data_type='string',
        description=''
    )

    cde_store_code = ColumnSpec(
        data_type='string',
        description=''
    )

    cde_base_sponsor_key = ColumnSpec(
        data_type='string',
        description=''
    )

    cde_base_bu_key = ColumnSpec(
        data_type='string',
        description=''
    )

    cde_base_opco_key = ColumnSpec(
        data_type='string',
        description=''
    )

    cde_layered_sponsor_key = ColumnSpec(
        data_type='string',
        description=''
    )

    cde_layered_bu_key = ColumnSpec(
        data_type='string',
        description=''
    )

    cde_layered_opco_key = ColumnSpec(
        data_type='string',
        description=''
    )

    des_transaction_type = ColumnSpec(
        data_type='string',
        description=''
    )

    des_transaction_source = ColumnSpec(
        data_type='string',
        description=''
    )

    cde_original_txn_key = ColumnSpec(
        data_type='string',
        description=''
    )

    cua_pay_in_currency = ColumnSpec(
        data_type='string',
        description=''
    )

    cua_pay_in_points_value = ColumnSpec(
        data_type='string',
        description=''
    )

    cua_tax_amount = ColumnSpec(
        data_type='string',
        description=''
    )

    cde_payment_method_key = ColumnSpec(
        data_type='string',
        description=''
    )

    qty_points_earned_basic = ColumnSpec(
        data_type='string',
        description=''
    )

    qty_points_earned_status = ColumnSpec(
        data_type='string',
        description=''
    )

    qty_redeemed_basic = ColumnSpec(
        data_type='string',
        description=''
    )

    qty_redeemed_status = ColumnSpec(
        data_type='string',
        description=''
    )

    cua_base_amount_points = ColumnSpec(
        data_type='string',
        description=''
    )

    cua_layered_amount_points = ColumnSpec(
        data_type='string',
        description=''
    )

    cua_bonus_amount_points = ColumnSpec(
        data_type='string',
        description=''
    )

    qty_bonus_offers = ColumnSpec(
        data_type='string',
        description=''
    )

    cde_line_position = ColumnSpec(
        data_type='string',
        description=''
    )

    cde_error = ColumnSpec(
        data_type='string',
        description=''
    )

    des_error = ColumnSpec(
        data_type='string',
        description=''
    )

    ind_is_test = ColumnSpec(
        data_type='string',
        description=''
    )

    ind_is_error = ColumnSpec(
        data_type='string',
        description=''
    )

    ind_is_manual = ColumnSpec(
        data_type='string',
        description=''
    )

    ind_is_cancellation = ColumnSpec(
        data_type='string',
        description=''
    )

    ind_is_reversed = ColumnSpec(
        data_type='string',
        description=''
    )

    ind_is_purchase = ColumnSpec(
        data_type='string',
        description=''
    )

    ind_is_third_party = ColumnSpec(
        data_type='string',
        description=''
    )

    idi_load = ColumnSpec(
        data_type='string',
        description=''
    )

    idi_ods = ColumnSpec(
        data_type='string',
        description=''
    )

    nam_tennant = ColumnSpec(
        data_type='string',
        description=''
    )

    nam_mall = ColumnSpec(
        data_type='string',
        description=''
    )

    cod_sor_proposition = ColumnSpec(
        data_type='string',
        description=''
    )

    cdi_turnover_status = ColumnSpec(
        data_type='string',
        description=''
    )

    des_turnover_status = ColumnSpec(
        data_type='string',
        description=''
    )

    cdi_date_type_1 = ColumnSpec(
        data_type='string',
        description=''
    )

    cdi_date_type_2 = ColumnSpec(
        data_type='string',
        description=''
    )

    dat_date_type_2 = ColumnSpec(
        data_type='date',
        description=''
    )

    cdi_date_type_3 = ColumnSpec(
        data_type='string',
        description=''
    )

    dat_date_type_3 = ColumnSpec(
        data_type='date',
        description=''
    )

    qty_number_of_items = ColumnSpec(
        data_type='string',
        description=''
    )

    cdi_item_unit = ColumnSpec(
        data_type='string',
        description=''
    )

    cde_currency = ColumnSpec(
        data_type='string',
        description=''
    )

    cdi_amount_type_1 = ColumnSpec(
        data_type='string',
        description=''
    )

    cua_amount_type_1 = ColumnSpec(
        data_type='string',
        description=''
    )

    cdi_amount_type_2 = ColumnSpec(
        data_type='string',
        description=''
    )

    cua_amount_type_2 = ColumnSpec(
        data_type='string',
        description=''
    )

    cdi_amount_type_3 = ColumnSpec(
        data_type='string',
        description=''
    )

    cua_amount_type_3 = ColumnSpec(
        data_type='string',
        description=''
    )

    cdi_amount_channel = ColumnSpec(
        data_type='string',
        description=''
    )

    des_amount_channel = ColumnSpec(
        data_type='string',
        description=''
    )

    att_turnover_classification_1 = ColumnSpec(
        data_type='string',
        description=''
    )

    cdi_turnover_classification_1 = ColumnSpec(
        data_type='string',
        description=''
    )

    des_turnover_classification_1 = ColumnSpec(
        data_type='string',
        description=''
    )

    att_turnover_classification_2 = ColumnSpec(
        data_type='string',
        description=''
    )

    cdi_turnover_classification_2 = ColumnSpec(
        data_type='string',
        description=''
    )

    des_turnover_classification_2 = ColumnSpec(
        data_type='string',
        description=''
    )

    att_turnover_classification_3 = ColumnSpec(
        data_type='string',
        description=''
    )

    cdi_turnover_classification_3 = ColumnSpec(
        data_type='string',
        description=''
    )

    des_turnover_classification_3 = ColumnSpec(
        data_type='string',
        description=''
    )

    idi_promotion = ColumnSpec(
        data_type='string',
        description=''
    )

    cod_sor_promotion = ColumnSpec(
        data_type='string',
        description=''
    )

    cdi_supplier = ColumnSpec(
        data_type='string',
        description=''
    )

    des_supplier = ColumnSpec(
        data_type='string',
        description=''
    )

    cde_promotion_type_key = ColumnSpec(
        data_type='string',
        description=''
    )

    cua_item_cost_price = ColumnSpec(
        data_type='string',
        description=''
    )

    cua_regular_sales_price = ColumnSpec(
        data_type='string',
        description=''
    )

    cua_promotion_sales_price = ColumnSpec(
        data_type='string',
        description=''
    )

    cua_margin_daily_sales = ColumnSpec(
        data_type='string',
        description=''
    )

    qty_daily_stock = ColumnSpec(
        data_type='string',
        description=''
    )

    cua_daily_stock = ColumnSpec(
        data_type='string',
        description=''
    )

    qty_daily_sales = ColumnSpec(
        data_type='string',
        description=''
    )

    cde_warehouse_number = ColumnSpec(
        data_type='string',
        description=''
    )

    des_supplier_type = ColumnSpec(
        data_type='string',
        description=''
    )

    cde_assortment_code_gima = ColumnSpec(
        data_type='string',
        description=''
    )

    cde_assortment_key = ColumnSpec(
        data_type='string',
        description=''
    )

    des_item_supplier_status = ColumnSpec(
        data_type='string',
        description=''
    )

    cde_supplier_key = ColumnSpec(
        data_type='string',
        description=''
    )

    cde_supplier_number = ColumnSpec(
        data_type='string',
        description=''
    )

    des_omni_channel_level02 = ColumnSpec(
        data_type='string',
        description=''
    )

    des_omni_channel_level03 = ColumnSpec(
        data_type='string',
        description=''
    )

    des_store_category = ColumnSpec(
        data_type='string',
        description=''
    )

    des_segment_type = ColumnSpec(
        data_type='string',
        description=''
    )

    des_category_group = ColumnSpec(
        data_type='string',
        description=''
    )

    des_store_experience = ColumnSpec(
        data_type='string',
        description=''
    )

    tim_trx_ts = ColumnSpec(
        data_type='string',
        description=''
    )

    des_order_status = ColumnSpec(
        data_type='string',
        description=''
    )

    des_company = ColumnSpec(
        data_type='string',
        description=''
    )

    idi_item = ColumnSpec(
        data_type='string',
        description=''
    )

    idi_brand_code = ColumnSpec(
        data_type='string',
        description=''
    )

    cua_net_price = ColumnSpec(
        data_type='string',
        description=''
    )

    cua_net_markdown_amount = ColumnSpec(
        data_type='string',
        description=''
    )

    cua_net_gross_profit = ColumnSpec(
        data_type='string',
        description=''
    )

    cua_net_discount_amount = ColumnSpec(
        data_type='string',
        description=''
    )

    idi_offer = ColumnSpec(
        data_type='string',
        description=''
    )

    idi_type = ColumnSpec(
        data_type='string',
        description=''
    )

    idi_tender_type = ColumnSpec(
        data_type='string',
        description=''
    )

    cua_net_amount_local = ColumnSpec(
        data_type='string',
        description=''
    )

    cua_net_gross_amount_local = ColumnSpec(
        data_type='string',
        description=''
    )

    des_discount_category = ColumnSpec(
        data_type='string',
        description=''
    )

    idi_promotion_code = ColumnSpec(
        data_type='string',
        description=''
    )

    qty_net_sold_discount = ColumnSpec(
        data_type='string',
        description=''
    )

    des_product_category = ColumnSpec(
        data_type='string',
        description=''
    )

    dat_session_start_time = ColumnSpec(
        data_type='string',
        description=''
    )

    dat_session_end_time = ColumnSpec(
        data_type='string',
        description=''
    )

    idi_movie_id = ColumnSpec(
        data_type='string',
        description=''
    )

    idi_movie_category_id = ColumnSpec(
        data_type='string',
        description=''
    )

    des_movie_language = ColumnSpec(
        data_type='string',
        description=''
    )

    des_movie_name = ColumnSpec(
        data_type='string',
        description=''
    )

    des_movie_category = ColumnSpec(
        data_type='string',
        description=''
    )

    idi_product_category_id = ColumnSpec(
        data_type='string',
        description=''
    )


