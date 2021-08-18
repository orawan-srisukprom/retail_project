#!/usr/bin/env python3

# Copyright 2016 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import apache_beam as beam
import csv

def run(project, bucket, dataset, region):
   argv = [
      '--project={0}'.format(project),
      '--job_name=ch04timecorr',
      '--save_main_session',
      '--staging_location=gs://{0}/flights/staging/'.format(bucket), #change name folder
      '--temp_location=gs://{0}/flights/temp/'.format(bucket), #change name folder
      '--setup_file=./setup.py',
      '--max_num_workers=8',
      '--region={}'.format(region),
      '--autoscaling_algorithm=THROUGHPUT_BASED',
      '--runner=DataflowRunner'
   ]
   retail_raw_files = 'gs://{}/retail/Online Retail.csv'.format(bucket)
   
   rfm_output = '{}:{}.rfm'.format(project, dataset)

   pipeline = beam.Pipeline(argv=argv)
   
   retail = (pipeline 
      | 'flights:read' >> beam.io.ReadFromText (retail_raw_files)
      )
    # (retail 
    #     | 'flights:tostring' >> beam.Map(lambda fields: ','.join(fields)) 
    #     #| 'flights:out' >> beam.io.textio.WriteToText(flights_output)
    # )

    rfm = retail | beam.FlatMap(get_rfm) #change function

    schema = 'CustomerID:INTEGER,Amount:float,Frequency:INTEGER, Recency:INTEGER'

    (rfm 
        #| 'rfm:totablerow' >> beam.Map(lambda fields: create_row(fields)) 
        | 'rfm:out' >> beam.io.WriteToBigQuery(
                                rfm_output, schema=schema,
                                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
    )

    pipeline.run()

if __name__ == '__main__':
   import argparse
   parser = argparse.ArgumentParser(description='Run pipeline on the cloud')
   parser.add_argument('-p','--project', help='Unique project ID', required=True)
   parser.add_argument('-b','--bucket', help='Bucket where your data were ingested in Chapter 2', required=True)
   parser.add_argument('-r','--region', help='Region in which to run the Dataflow job. Choose the same region as your bucket.', required=True)
   parser.add_argument('-d','--dataset', help='BigQuery dataset', default='flights')
   args = vars(parser.parse_args())

   print ("Correcting timestamps and writing to BigQuery dataset {}".format(args['dataset']))
  
   run(project=args['project'], bucket=args['bucket'], dataset=args['dataset'], region=args['region'])
