from enum import Enum

from cvmdatalake import TableSpec, ColumnSpec


class FlexibleProfileManager(TableSpec):

    @classmethod
    def table_description(cls):
        return "Table of status perpare job, learnig and transform jobs and files needed for prepare data for custom customer profile"

    name = ColumnSpec(
        data_type='string',
        description='Name of task, example "cfr_kids"'
    )

    status = ColumnSpec(
        data_type='string',
        description='Status of all task. Possible values: "not_started", "in_progress", "finished", "failed"'
    )

    date_of_dataset = ColumnSpec(
        data_type='date',
        description='last date of transaction of data which was used to train data'
    )

    requirement_product_bu = ColumnSpec(
        data_type='string',
        description='Filter on BU from transaction table'
    )

    requirement_nam_proposition_level03 = ColumnSpec(
        data_type='string',
        description='Filter nam_proposition_level03 from transaction table'
    )

    requirement_date_from = ColumnSpec(
        data_type='date',
        description='If empty then it is from beginning of time'
    )

    requirement_date_to = ColumnSpec(
        data_type='date',
        description='If empty, then it is current date'
    )

    nbo_status = ColumnSpec(
        data_type='string',
        description='Status of NBO. Possible values: "not_started", "in_progress", "finished", "failed"'
    )

    nbo_date_training = ColumnSpec(
        data_type='date',
        description='Date of last update of this record'
    )

    nbo_path_train_data = ColumnSpec(
        data_type='string',
        description='Path to generated train data files'
    )

    clv_status = ColumnSpec(
        data_type='string',
        description='Status of clv. Possible values: "not_started", "in_progress", "finished", "failed"'
    )

    clv_status_input_train = ColumnSpec(
        data_type='string',
        description='clv_status_input_train'
    )

    clv_status_input_transform = ColumnSpec(
        data_type='string',
        description='clv_status_input_transform'
    )

    clv_date_model_training = ColumnSpec(
        data_type='date',
        description='Date of last update of this record'
    )

    clv_path_train_data = ColumnSpec(
        data_type='string',
        description='Path to generated train data files'
    )
