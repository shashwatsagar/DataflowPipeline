{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 2,
   "id": "frequent-marsh",
   "metadata": {},
   "outputs": [],
   "source": []
  },
  {
   "cell_type": "code",
   "execution_count": 2,
   "id": "quick-tolerance",
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
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "WARNING:apache_beam.options.pipeline_options:Discarding unparseable args: ['-f', '/root/.local/share/jupyter/runtime/kernel-6dd8930f-c3fd-42dc-86ee-c35db9cbb51c.json']\n"
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
   "execution_count": 4,
   "id": "characteristic-madagascar",
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
   "id": "deluxe-sandwich",
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
