from cvmdatalake import TableSpec, ColumnSpec


class Offer(TableSpec):

    @classmethod
    def table_description(cls):
        return "Offer from Offers bank from table offerbank_offers"

    id = ColumnSpec(
        data_type='int'
    )

    offer_name = ColumnSpec(
        data_type='string'
    )
