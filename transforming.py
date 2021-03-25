{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 42,
   "id": "dutch-filter",
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
      "WARNING:apache_beam.options.pipeline_options:Discarding unparseable args: ['-f', '/root/.local/share/jupyter/runtime/kernel-f274799a-1ae8-4476-9aa3-7a800ca1d8a2.json']\n"
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
    "PROJECT = 'tokyo-botany-302620'\n",
    "schema1 = 'neighbourhood:STRING,total_count:NUMERIC'\n",
    "    \n",
    "def collectNeighbourhood(data):\n",
    "    yield '{},{}'.format(data['neighbourhood'],data['id'])\n",
    "\n",
    "def discard_incomplete(data):\n",
    "    \"\"\"Filters out records that don't have an information.\"\"\"\n",
    "    return len(data[0]) > 0 and len(data[1]) > 0 and len(data[2]) > 0 and len(data[3]) > 0 and len(data[4]) > 0 and len(data[5]) > 0 and len(data[6]) > 0 and len(data[7]) > 0 and len(data[8]) > 0 and len(data[9]) > 0 and len(data[10]) > 0 and len(data[11]) > 0 and len(data[12]) > 0 and len(data[13]) > 0 and len(data[14]) > 0 and len(data[15]) > 0\n",
    "    \n",
    "def transform(argv=None):\n",
    "    inputfile = 'gs://airbnbnyc2019/AB_NYC_2019 (1).csv'\n",
    "    outputfile = 'gs://airbnbnyc2019/output/output.csv'\n",
    "\n",
    "    pipeline_options = PipelineOptions()\n",
    "    pipeline_options.view_as(SetupOptions).save_main_session = True\n",
    "    p = beam.Pipeline(options=pipeline_options)\n",
    "    \n",
    "    lines = p | 'ReadMyFile' >> beam.io.ReadFromText(inputfile)\n",
    "    \n",
    "    (lines\n",
    "        | 'Parse CSV1' >> beam.Regex.replace_all(r'\\\"([^\\\"]*)\\\"',lambda x:x.group(1).replace(',',''))\n",
    "        | 'Split11' >> beam.Map(lambda x: x.split(','))\n",
    "        | 'DeleteIncompleteData1' >> beam.Filter(discard_incomplete)\n",
    "        | 'format to dict' >> beam.Map(lambda x: {\"id\": x[0], \"name\": x[1], \"host_id\": x[2], \"host_name\": x[3], \"neighbourhood_group\": x[4], \"neighbourhood\": x[5], \"latitude\": x[6], \"longitude\": x[7], \"room_type\": x[8],\"price\": x[9], \"minimum_nights\": x[10], \"number_of_reviews\":x[11],\"last_review\":x[12],\"reviews_per_month\":x[13],\"calculated_host_listings_count\":x[14], \"availability_365\":x[15]})\n",
    "        | 'Transform' >> beam.FlatMap(collectNeighbourhood)\n",
    "        | 'Split2' >> beam.Map(lambda x: x.split(','))\n",
    "     \n",
    "        | 'CombineNeighbourhood' >> beam.combiners.Count.PerKey()\n",
    "        | 'format to dict2' >> beam.Map(lambda y: {\"neighbourhood\": y[0], \"total_count\": y[1]})\n",
    "        #| 'Print' >> beam.Map(print)\n",
    "        | 'WriteToBigQueryCount' >> beam.io.WriteToBigQuery('{0}:nycairbnb.airbnb_neighbourhood_count'.format(PROJECT), schema=schema1,\n",
    "                                                        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,method=\"STREAMING_INSERTS\")\n",
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
   "execution_count": null,
   "id": "saving-helmet",
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
