import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
import xml.etree.ElementTree as ET

class ParseXML(beam.DoFn):
    def process(self, element):
        # element = entire XML file
        root = ET.fromstring(element)
        for rec in root.findall("record"):
            yield {
                "id": int(rec.find("id").text),
                "name": rec.find("name").text,
                "email": rec.find("email").text,
                "last_updated": rec.find("last_updated").text
            }

def run():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--input")
    parser.add_argument("--output_table")

    args, beam_args = parser.parse_known_args()

    options = PipelineOptions(
        beam_args,
        save_main_session=True
    )

    with beam.Pipeline(options=options) as p:

        (p
         | "Read XML" >> beam.io.ReadFromText(args.input)
         | "Parse XML" >> beam.ParDo(ParseXML())
         | "WriteToBQ" >> beam.io.WriteToBigQuery(
                args.output_table,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )

if __name__ == "__main__":
    run()
