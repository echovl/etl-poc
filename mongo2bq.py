import apache_beam as beam
import argparse
from apache_beam.options.pipeline_options import PipelineOptions

class MapFields(beam.DoFn):
    def process(self, element):
        res = {
            "id": str(element["_id"]),
            "name": element["productName"],
            "created_at": element["createdAt"],
            "updated_at": element["updatedAt"],
        }

        return [res]


menus_db = "mongodb+srv://eodevstore:eodevstore@poc-zw0qt.mongodb.net"

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
                uri=menus_db,
                db="dev",
                coll="products",
                bucket_auto=True,
                filter={
                    "storeId": "5d9b8d35449cd0001d4e1ff0",
                    "status": 1,
                }
            ) | beam.ParDo(MapFields())


        docs | beam.io.WriteToBigQuery(
            known_args.output,
            schema='id:string, name:STRING, created_at:INTEGER, updated_at:integer',
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )

run()
