{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 2,
   "id": "industrial-opening",
   "metadata": {},
   "outputs": [],
   "source": []
  },
  {
   "cell_type": "code",
   "execution_count": 1,
   "id": "endangered-proposal",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Started Running\n"
     ]
    },
    {
     "data": {
      "application/javascript": [
       "\n",
       "        if (typeof window.interactive_beam_jquery == 'undefined') {\n",
       "          var jqueryScript = document.createElement('script');\n",
       "          jqueryScript.src = 'https://code.jquery.com/jquery-3.4.1.slim.min.js';\n",
       "          jqueryScript.type = 'text/javascript';\n",
       "          jqueryScript.onload = function() {\n",
       "            var datatableScript = document.createElement('script');\n",
       "            datatableScript.src = 'https://cdn.datatables.net/1.10.20/js/jquery.dataTables.min.js';\n",
       "            datatableScript.type = 'text/javascript';\n",
       "            datatableScript.onload = function() {\n",
       "              window.interactive_beam_jquery = jQuery.noConflict(true);\n",
       "              window.interactive_beam_jquery(document).ready(function($){\n",
       "                \n",
       "              });\n",
       "            }\n",
       "            document.head.appendChild(datatableScript);\n",
       "          };\n",
       "          document.head.appendChild(jqueryScript);\n",
       "        } else {\n",
       "          window.interactive_beam_jquery(document).ready(function($){\n",
       "            \n",
       "          });\n",
       "        }"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "/root/apache-beam-2.28.0/lib/python3.7/site-packages/apache_beam/io/gcp/bigquery.py:1653: BeamDeprecationWarning: options is deprecated since First stable release. References to <pipeline>.options will not be supported\n",
      "  experiments = p.options.view_as(DebugOptions).experiments or []\n",
      "WARNING:apache_beam.options.pipeline_options:Discarding unparseable args: ['-f', '/root/.local/share/jupyter/runtime/kernel-84838be0-5406-43e0-95c1-89145089a1ef.json']\n"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Completed Running\n"
     ]
    }
   ],
   "source": [
    "import apache_beam as beam\n",
    "from apache_beam.io import ReadFromText\n",
    "from apache_beam.io import WriteToText\n",
    "from google.cloud import bigquery\n",
    "from apache_beam.options.pipeline_options import PipelineOptions\n",
    "from apache_beam.options.pipeline_options import SetupOptions\n",
    "import re\n",
    "\n",
    "# pip3 install apache-beam[gcp]\n",
    "\n",
    "# python main.py --setup_file setup.py\n",
    "\n",
    "# python3 main.py --project=playground-s-11-0fff7bed --job_name=dataflow-demo \n",
    "# --save_main_session --requirements_file=requirements.txt --staging_location=gs://bucket_store123/staging/ \n",
    "# --temp_location=gs://bucket_store123/temp/ --region=us-central1 --worker_region=us-central1 --runner=DataflowRunner \n",
    "# --template_location=gs://bucket_store123/templates/dataflow-poc\n",
    "\n",
    "# python3 main.py --project=playground-s-11-0fff7bed --job_name=dataflow-demo --staging_location=gs://bucket_store123/staging/ \n",
    "# --temp_location=gs://bucket_store123/temp/ --region=us-central1 --worker_region=us-central1 --runner=DataflowRunner \n",
    "\n",
    "PROJECT=\"tokyo-botany-302620\"\n",
    "schema = 'id: NUMERIC, name: STRING, host_id: NUMERIC, host_name: STRING, neighbourhood_group: STRING, neighbourhood: SRTING, latitude: FLOAT,longitude: FLOAT,room_type: STRING,price: INTEGER, minimum_nights: INTEGER, number_of_reviews: INTEGER, last_review: DATE, reviews_per_month: FLOAT, calculated_host_listings_count: INTEGER,availability_365: INTEGER'\n",
    "schema1 = 'neighbourhood: STRING, count:NUMERIC'\n",
    "    \n",
    "def collectNeighbourhood(data):\n",
    "    yield '{},{}'.format(data['neighbourhood'],data['id'])\n",
    "\n",
    "def discard_incomplete(data):\n",
    "    \"\"\"Filters out records that don't have an information.\"\"\"\n",
    "    return len(data[0]) > 0 and len(data[1]) > 0 and len(data[2]) > 0 and len(data[3]) > 0 and len(data[4]) > 0 and len(data[5]) > 0 and len(data[6]) > 0 and len(data[7]) > 0 and len(data[8]) > 0 and len(data[9]) > 0 and len(data[10]) > 0 and len(data[11]) > 0 and len(data[12]) > 0 and len(data[13]) > 0 and len(data[14]) > 0 and len(data[15]) > 0\n",
    "    \n",
    "    \n",
    "def transform(argv=None):\n",
    "\n",
    "    inputfile = 'gs://airbnbnyc2019/AB_NYC_2019 (1).csv'\n",
    "\n",
    "    pipeline_options = PipelineOptions()\n",
    "    pipeline_options.view_as(SetupOptions).save_main_session = True\n",
    "    p = beam.Pipeline(options=pipeline_options)\n",
    "    \n",
    "    lines = p | 'ReadMyFile' >> beam.io.ReadFromText(inputfile)\n",
    "    \n",
    "    (lines\n",
    "        | 'Parse CSV' >> beam.Regex.replace_all(r'\\\"([^\\\"]*)\\\"',lambda x:x.group(1).replace(',',''))\n",
    "        | 'Split' >> beam.Map(lambda x: x.split(','))\n",
    "        | 'DeleteIncompleteData' >> beam.Filter(discard_incomplete)\n",
    "        | 'format to dict' >> beam.Map(lambda x: {\"id\": x[0], \"name\": x[1], \"host_id\": x[2], \"host_name\": x[3], \"neighbourhood_group\": x[4], \"neighbourhood\": x[5], \"latitude\": x[6], \"longitude\": x[7], \"room_type\": x[8],\"price\": x[9], \"minimum_nights\": x[10], \"number_of_reviews\":x[11],\"last_review\":x[12],\"reviews_per_month\":x[13],\"calculated_host_listings_count\":x[14], \"availability_365\":x[15]})\n",
    "\n",
    "        | 'WriteToBigQuery' >> beam.io.WriteToBigQuery('{0}:nycairbnb.airbnb_nyc'.format(PROJECT),schema=schema,\n",
    "                                                       write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND, method=\"STREAMING_INSERTS\")\n",
    "    )\n",
    "    p.run()\n",
    "\n",
    "if __name__ == '__main__':\n",
    "    print('Started Running')\n",
    "    transform()\n",
    "    print('Completed Running')\n",
    "    "
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 1,
   "id": "impaired-feeding",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "python3: can't open file 'main.py': [Errno 2] No such file or directory\n"
     ]
    }
   ],
   "source": []
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "present-symposium",
   "metadata": {},
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Apache Beam 2.28.0 for Python 3",
   "language": "python",
   "name": "apache-beam-2.28.0"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.7.9"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
