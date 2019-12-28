import os
import re
import csv
import json
import traceback
from time import sleep
from urllib.parse import unquote

import boto3

athena = boto3.client('athena')
s3 = boto3.client('s3')
db_name = os.getenv('AthenaDbName')
output_bucket = os.getenv('OutputBucket')
create_table_tpl = """
CREATE EXTERNAL TABLE IF NOT EXISTS
  {}.{} {}
  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
  WITH SERDEPROPERTIES ('serialization.format' = ',', 'field.delim' = ',')
  LOCATION '{}'
  TBLPROPERTIES ('skip.header.line.count'='1');
"""


def query_status(qid):
    return athena.get_query_execution(QueryExecutionId=qid)['QueryExecution']['Status']['State']


def wait_query_to_finish(qid):
    while query_status(qid) in ['QUEUED', 'RUNNING']:
        print("Waiting query id to finish {}".format(qid))
        sleep(5)
    print("query id finished {}".format(qid))


def run_query(query):
    print("Running query: {}".format(query))
    qid = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': db_name},
        ResultConfiguration={'OutputLocation': output_bucket}
    )['QueryExecutionId']
    print("query id: {}".format(qid))
    return qid


def handler(event, context):
    print(json.dumps(event))

    try:
        query = "CREATE DATABASE IF NOT EXISTS {};".format(db_name)
        query_id = run_query(query)
        wait_query_to_finish(query_id)

        for r in event['Records']:
            bucket = r['s3']['bucket']['name']
            key = r['s3']['object']['key'].replace('+', ' ')
            if 'view' in key: # Skip for files under view dir
                continue
            csv_file = '/tmp/' + os.path.basename(key)
            csv_path = os.path.dirname(key)
            table_name = re.sub('[^a-z0-9_]+', '_', csv_path.split('/')[-1].lower())
            location = 's3://{}/{}/'.format(bucket, csv_path)
            s3.download_file(bucket, unquote(key), csv_file)
            columns = []
            with open(csv_file, 'r') as f:
                for i in csv.DictReader(f).fieldnames:
                    columns.append('`' + re.sub('[^a-z0-9-]+', '-', i.lower()) + '` string')
            columns = '(' + ', ' . join(columns) + ')'
            
            query = create_table_tpl.format(db_name, table_name, columns, location)
            query_id = run_query(query)
            wait_query_to_finish(query_id)
            
            x = csv_path.split('/')[:-1]
            x.append('views/')
            view_path = '/' . join(x)
            files = s3.list_objects(Bucket=bucket, Prefix=view_path)
            if 'Contents' in files.keys():
                for k, f in enumerate(files['Contents']):
                    if f['Key'].endswith('/'):
                        continue
                    view_file = '/tmp/' + os.path.basename(f['Key'])
                    s3.download_file(bucket, f['Key'], view_file)
                    with open(view_file, 'r') as v:
                        view_name = table_name + '_' + str(k) + '_view'
                        query = v.read().format(view_name, table_name)
                        query_id = run_query(query)
                        wait_query_to_finish(query_id)
    except Exception as e:
        print(str(e))
        traceback.print_exc()
    return 0
