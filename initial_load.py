import apache_beam as beam
import argparse
import logging
from apache_beam.options.pipeline_options import PipelineOptions


def money_to_integer(amount):
    if amount:
        return int(amount*100)
    else:
        return None


class OrderSchema2Table(beam.DoFn):
    def process(self, element):

        res = {
            "id": str(element["_id"]),
            "number": element.get("orderNumber"),
            "sell_status": element.get("orderSellStatus"),
            "serve_status": element.get("orderServeStatus"),
            "type": element.get("orderType"),
            "tax_amount": money_to_integer(element.get("taxAmount")),
            "discount_amount": money_to_integer(element.get("discountAmount")),
            "delivery_amount": money_to_integer(element.get("deliveryAmount")),
            "subtotal_amount": money_to_integer(element.get("subTotalAmount")),
            "total_amount": money_to_integer(element.get("totalAmount")),
            "store_id": element.get("storeId"),
            "merchant_id": element.get("merchantId"),
            "created_at": element.get("createdAt"),
            "updated_at": element.get("updatedAt")
        }

        return [res]


orders_db = "mongodb+srv://eodevstore:eodevstore@poc-7aaiu.mongodb.net"
order_table_schema = (
    'id:STRING,number:INTEGER,'
    'sell_status:INTEGER,'
    'serve_status:INTEGER,'
    'type:INTEGER,'
    'tax_amount:INTEGER,'
    'discount_amount:INTEGER,'
    'delivery_amount:INTEGER,'
    'subtotal_amount:INTEGER,'
    'total_amount:INTEGER,'
    'store_id:STRING,'
    'merchant_id:STRING,'
    'created_at:INTEGER,'
    'updated_at:INTEGER'
)


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input',
                        dest='input',
                        help='File to read in.')
    parser.add_argument('--output',
                        dest='output',
                        help='BigQuery output dataset and table name in the format dataset.tablename')
    known_args, pipeline_args = parser.parse_known_args(argv)

    options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=options) as p:
        docs = p | beam.io.ReadFromMongoDB(
            uri=orders_db,
            db="dev",
            coll="orders",
            bucket_auto=True,
            filter={"status": 1}
        ) | beam.ParDo(OrderSchema2Table())

        docs | beam.io.WriteToBigQuery(
            known_args.output,
            schema=order_table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )
        #docs | beam.io.WriteToText("result.txt")


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
