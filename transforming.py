# -*- coding: utf-8 -*-
"""transforming.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1ZMkw6GvU0AACi2HzHHV17kJ6KKqVzpnZ
"""

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from google.cloud import bigquery
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
import re

PROJECT = 'tokyo-botany-302620'
schema1 = 'neighbourhood:STRING,total_count:NUMERIC'
TOPIC=' ps-to-bq-airbnbtransaction1'

def collectNeighbourhood(data):
    yield '{},{}'.format(data['neighbourhood'],data['id'])

def discard_incomplete(data):
    """Filters out records that don't have an information."""
    return len(data[0]) > 0 and len(data[1]) > 0 and len(data[2]) > 0 and len(data[3]) > 0 and len(data[4]) > 0 and len(data[5]) > 0 and len(data[6]) > 0 and len(data[7]) > 0 and len(data[8]) > 0 and len(data[9]) > 0 and len(data[10]) > 0 and len(data[11]) > 0 and len(data[12]) > 0 and len(data[13]) > 0 and len(data[14]) > 0 and len(data[15]) > 0
    
def transform(argv=None):
    inputfile = 'gs://airbnbnyc2019/AB_NYC_2019 (1).csv'
    #outputfile = 'gs://airbnbnyc2019/output/output1.csv'
    
    pipeline_options = PipelineOptions(flags=argv,
    runner='DataflowRunner',
    project='tokyo-botany-302620',
    job_name='groupby-neighbourhood',
    temp_location='gs://airbnbnyc2019/temp/',
    region='us-central1')
    #pipeline_options.view_as(SetupOptions).save_main_session = True
    p = beam.Pipeline(options=pipeline_options)
    
    lines = p | 'ReadMyFile' >> beam.io.ReadFromText(inputfile)
    
    (lines
        #| 'PubSub' >> beam.io.ReadFromPubSub(topic=TOPIC).with_output_types(bytes)
        | 'Parse CSV1' >> beam.Regex.replace_all(r'\"([^\"]*)\"',lambda x:x.group(1).replace(',',''))
        | 'Split11' >> beam.Map(lambda x: x.split(','))
        | 'DeleteIncompleteData1' >> beam.Filter(discard_incomplete)
        | 'format to dict' >> beam.Map(lambda x: {"id": x[0], "name": x[1], "host_id": x[2], "host_name": x[3], "neighbourhood_group": x[4], "neighbourhood": x[5], "latitude": x[6], "longitude": x[7], "room_type": x[8],"price": x[9], "minimum_nights": x[10], "number_of_reviews":x[11],"last_review":x[12],"reviews_per_month":x[13],"calculated_host_listings_count":x[14], "availability_365":x[15]})
        | 'Transform' >> beam.FlatMap(collectNeighbourhood)
        | 'Split2' >> beam.Map(lambda x: x.split(','))
     
        | 'CombineNeighbourhood' >> beam.combiners.Count.PerKey()
        | 'format to dict2' >> beam.Map(lambda y: {"neighbourhood": y[0], "total_count": y[1]})
        | 'WriteToBigQueryCount' >> beam.io.WriteToBigQuery('{0}:nycairbnb.airbnb_neighbourhood_count'.format(PROJECT), schema=schema1,
                                                        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,method="STREAMING_INSERTS")
    )
    p.run().wait_until_finish()

if __name__ == '__main__':
    print('Started Running')
    transform()
    print('Completed Running')