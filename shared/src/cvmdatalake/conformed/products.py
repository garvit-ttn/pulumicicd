from enum import Enum

from cvmdatalake import TableSpec, ColumnSpec


class Products(TableSpec):

    @classmethod
    def table_description(cls):
        return "Input Products data provided by MAF"

    bu = ColumnSpec(
        is_partition=True,
        data_type='string',
        description="Business Unit partition"
    )

    dat_snapshot = ColumnSpec(
        data_type='date',
        description="Snapshot date, the date on which the data was captured and exported from the System of Record (SOR) to the file. The Snapshot date is the same for every record in the File"
    )

    idi_proposition = ColumnSpec(
        data_type='string',
        description="Unique ID of proposition in specific source system"
    )

    cod_sor_proposition = ColumnSpec(
        data_type='string',
        description="Numeric ID of the System of Record (SOR) where your IDI_PROPOSITION is recorded"
    )

    cdi_proposition_type = ColumnSpec(
        data_type='string',
        description="Code for a type of Proposition that is used inside your Organization: Example PRODUCT, SERVICE, SUBSCRIPTION, etc"
    )

    des_proposition_type = ColumnSpec(
        data_type='string',
        description="Description for a type of Proposition that is used inside your Organization: Example Product, Service, Subscription, etc"
    )

    cdi_proposition_status = ColumnSpec(
        data_type='string',
        description="Code of status of proposition"
    )

    des_proposition_status = ColumnSpec(
        data_type='string',
        description="Description of status of proposition"
    )

    nam_proposition = ColumnSpec(
        data_type='string',
        description="Descripton of product or Service as known inside your organization"
    )

    dat_proposition_last_mutation = ColumnSpec(
        data_type='date',
        description="Date of last mutation on this record inside your SOR"
    )

    qty_proposition_number_of_levels = ColumnSpec(
        data_type='string',
        description="Number of levels of propositions (how many levels does the Hierarchy in the Proposition Three have inside your organization) Maximum = 8"
    )

    cdi_proposition_level = ColumnSpec(
        data_type='string',
        description="Code of level of this proposition in the Proposition Tree (Top Level = 1, Lower levels are 2-8)"
    )

    att_proposition_level01 = ColumnSpec(
        data_type='string',
        description="Name of the attribute inside your organization that is used for this Proposition on Level 1"
    )

    idi_proposition_level01 = ColumnSpec(
        data_type='string',
        description="Proposition ID on level 1. If not applicable Proposition ID , please use '_#_ 'as Default"
    )

    nam_proposition_level01 = ColumnSpec(
        data_type='string',
        description="Name of Proposition on level 1 , If not applicable Proposition Id use '_NOT_APPLICABLE_' as default"
    )

    att_proposition_level02 = ColumnSpec(
        data_type='string',
        description="Name of the attribute inside your organization that is used for this Proposition on Level 2"
    )

    idi_proposition_level02 = ColumnSpec(
        data_type='string',
        description="Proposition ID on level 2. If not applicable Proposition ID , please use '_#_ 'as Default"
    )

    nam_proposition_level02 = ColumnSpec(
        data_type='string',
        description="Name of Proposition on level 2 , If not applicable Proposition Id use '_NOT_APPLICABLE_' as default"
    )

    att_proposition_level03 = ColumnSpec(
        data_type='string',
        description="Name of the attribute inside your organization that is used for this Proposition on Level 3"
    )

    idi_proposition_level03 = ColumnSpec(
        data_type='string',
        description="Proposition ID on level 3. If not applicable Proposition ID , please use '_#_ 'as Default"
    )

    nam_proposition_level03 = ColumnSpec(
        data_type='string',
        description="Name of Proposition on level 3 , If not applicable Proposition Id use '_NOT_APPLICABLE_' as default"
    )

    att_proposition_level04 = ColumnSpec(
        data_type='string',
        description="Name of the attribute inside your organization that is used for this Proposition on Level 4"
    )

    idi_proposition_level04 = ColumnSpec(
        data_type='string',
        description="Proposition ID on level 4. If not applicable Proposition ID , please use '_#_ 'as Default"
    )

    nam_proposition_level04 = ColumnSpec(
        data_type='string',
        description="Name of Proposition on level 4 , If not applicable Proposition Id use '_NOT_APPLICABLE_' as default"
    )

    att_proposition_level05 = ColumnSpec(
        data_type='string',
        description="Name of the attribute inside your organization that is used for this Proposition on Level 5"
    )

    idi_proposition_level05 = ColumnSpec(
        data_type='string',
        description="Proposition ID on level 5. If not applicable Proposition ID , please use '_#_ 'as Default"
    )

    nam_proposition_level05 = ColumnSpec(
        data_type='string',
        description="Name of Proposition on level 5 , If not applicable Proposition Id use '_NOT_APPLICABLE_' as default"
    )

    att_proposition_level06 = ColumnSpec(
        data_type='string',
        description="Name of the attribute inside your organization that is used for this Proposition on Level 6"
    )

    idi_proposition_level06 = ColumnSpec(
        data_type='string',
        description="Proposition ID on level 6. If not applicable Proposition ID , please use '_#_ 'as Default"
    )

    nam_proposition_level06 = ColumnSpec(
        data_type='string',
        description="Name of Proposition on level 6 , If not applicable Proposition Id use '_NOT_APPLICABLE_' as default"
    )

    att_proposition_level07 = ColumnSpec(
        data_type='string',
        description="Name of the attribute inside your organization that is used for this Proposition on Level 7"
    )

    idi_proposition_level07 = ColumnSpec(
        data_type='string',
        description="Proposition ID on level 71. If not applicable Proposition ID , please use '_#_ 'as Default"
    )

    nam_proposition_level07 = ColumnSpec(
        data_type='string',
        description="Name of Proposition on level 7 , If not applicable Proposition Id use '_NOT_APPLICABLE_' as default"
    )

    att_proposition_level08 = ColumnSpec(
        data_type='string',
        description="Name of the attribute inside your organization that is used for this Proposition on Level 8"
    )

    idi_proposition_level08 = ColumnSpec(
        data_type='string',
        description="Proposition ID on level 8. If not applicable Proposition ID , please use '_#_ 'as Default"
    )

    nam_proposition_level08 = ColumnSpec(
        data_type='string',
        description="Name of Proposition on level 8 , If not applicable Proposition Id use '_NOT_APPLICABLE_' as default"
    )

    cdi_department = ColumnSpec(
        data_type='string',
        description="custome field"
    )

    des_department = ColumnSpec(
        data_type='string',
        description="custome field"
    )

    cde_brand = ColumnSpec(
        data_type='string',
        description="custome field"
    )

    des_brand = ColumnSpec(
        data_type='string',
        description="custome field"
    )

    cde_supplier_number = ColumnSpec(
        data_type='string',
        description="custome field"
    )

    des_supplier = ColumnSpec(
        data_type='string',
        description="custome field"
    )

    des_country_name = ColumnSpec(
        data_type='string',
        description="custome field"
    )

    des_item_type_name = ColumnSpec(
        data_type='string',
        description="custome field"
    )

    dat_creation_date = ColumnSpec(
        data_type='timestamp',
        description="custome field"
    )

    qty_item_net_weight = ColumnSpec(
        data_type='string',
        description="custome field"
    )

    qty_item_volume = ColumnSpec(
        data_type='string',
        description="custome field "
    )

    cde_item_range_code = ColumnSpec(
        data_type='string',
        description="custome field"
    )

    cde_item_unit_code = ColumnSpec(
        data_type='string',
        description="custome field"
    )

    qty_numner_of_pieces = ColumnSpec(
        data_type='string',
        description="custome field"
    )

    cde_ean_code = ColumnSpec(
        data_type='string',
        description="custome field"
    )

    dat_valid_from = ColumnSpec(
        data_type='date',
        description="custome field"
    )

    dat_valid_to = ColumnSpec(
        data_type='date',
        description="custome field"
    )

    cde_country_key = ColumnSpec(
        data_type='string',
        description="custome field"
    )

    dat_created = ColumnSpec(
        data_type='date',
        description="Date of creation of this record"
    )

    dat_update = ColumnSpec(
        data_type='date',
        description="Date of last update of this record"
    )

    cde_variant_id = ColumnSpec(
        data_type='string',
        description=''
    )

    cde_brand_code = ColumnSpec(
        data_type='string',
        description=''
    )

    cde_category_code = ColumnSpec(
        data_type='string',
        description=''
    )

    des_category_name = ColumnSpec(
        data_type='string',
        description=''
    )

    des_color_description = ColumnSpec(
        data_type='string',
        description=''
    )

    cde_color_id = ColumnSpec(
        data_type='string',
        description=''
    )

    des_company = ColumnSpec(
        data_type='string',
        description=''
    )

    cua_currentprice = ColumnSpec(
        data_type='string',
        description=''
    )

    des_division = ColumnSpec(
        data_type='string',
        description=''
    )

    cde_division_code = ColumnSpec(
        data_type='string',
        description=''
    )

    cde_dw_sourcecode = ColumnSpec(
        data_type='string',
        description=''
    )

    cde_inventdimid = ColumnSpec(
        data_type='string',
        description=''
    )

    cde_item_id = ColumnSpec(
        data_type='string',
        description=''
    )

    cua_originalprice = ColumnSpec(
        data_type='string',
        description=''
    )

    cde_product_key = ColumnSpec(
        data_type='string',
        description=''
    )

    cde_season_code = ColumnSpec(
        data_type='string',
        description=''
    )

    des_size_description = ColumnSpec(
        data_type='string',
        description=''
    )

    cde_size_id = ColumnSpec(
        data_type='string',
        description=''
    )

    des_supp_brandl1 = ColumnSpec(
        data_type='string',
        description=''
    )

    des_supp_brandseason = ColumnSpec(
        data_type='string',
        description=''
    )

    des_supp_collection = ColumnSpec(
        data_type='string',
        description=''
    )

    des_supp_fabric = ColumnSpec(
        data_type='string',
        description=''
    )

    des_supp_look = ColumnSpec(
        data_type='string',
        description=''
    )

    des_supp_story = ColumnSpec(
        data_type='string',
        description=''
    )

    des_link_url = ColumnSpec(
        data_type='string',
        description=''
    )

    des_image_link_url = ColumnSpec(
        data_type='string',
        description=''
    )

    des_feed_title = ColumnSpec(
        data_type='string',
        description=''
    )

    des_feed_link = ColumnSpec(
        data_type='string',
        description=''
    )

    des_feed_description = ColumnSpec(
        data_type='string',
        description=''
    )

    des_title = ColumnSpec(
        data_type='string',
        description=''
    )

    des_description = ColumnSpec(
        data_type='string',
        description=''
    )
