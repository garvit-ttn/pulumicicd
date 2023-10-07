from typing import Type

import pandas as pd
from pandas import ExcelWriter

from cvmdatalake import landing, conformed, TableSpec


def generate_schema(writer: ExcelWriter, table: Type[TableSpec]):
    names = list(map(lambda x: x.name, table.fields()))
    types = list(map(lambda x: x.data_type, table.fields()))
    desc = list(map(lambda x: x.description, table.fields()))
    part = list(map(lambda x: x.is_partition, table.fields()))

    df = pd.DataFrame(list(zip(names, types, part, desc)), columns=['name', 'type', 'partition', 'description'])

    df.index += 1
    df.to_excel(excel_writer=writer, sheet_name=table.table_id())


with pd.ExcelWriter('cvm_data_model_landing.xlsx') as w:
    generate_schema(w, landing.Transactions)
    generate_schema(w, landing.IdMapping)
    generate_schema(w, landing.Products)
    generate_schema(w, landing.Promotions)
    generate_schema(w, landing.Activities)
    generate_schema(w, landing.CustomAttributes)
    generate_schema(w, landing.RefSor)
    generate_schema(w, landing.Domains)

with pd.ExcelWriter('cvm_data_model_conformed.xlsx') as w:
    generate_schema(w, conformed.Demographics)
    generate_schema(w, conformed.Transactions)
    generate_schema(w, conformed.IdMapping)
    generate_schema(w, conformed.Products)
    generate_schema(w, conformed.Promotions)
    generate_schema(w, conformed.Activities)
    generate_schema(w, conformed.CustomAttributes)
    generate_schema(w, conformed.RefSor)
    generate_schema(w, conformed.Domains)
    generate_schema(w, conformed.CustomerProfile)
