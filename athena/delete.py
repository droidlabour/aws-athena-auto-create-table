import os
import re
import json
import traceback
from time import sleep
from datetime import datetime

import boto3

athena = boto3.client('athena')
dbName = os.getenv('AthenaDbName')
oputBucket = os.getenv('OutputBucket')


def queryStatus(qid):
    return athena.get_query_execution(QueryExecutionId=qid)['QueryExecution']['Status']['State']


def getQueryResult(qid):
    result = athena.get_query_results(QueryExecutionId=qid)
    if 'ResultSet' in result.keys():
        if 'Rows' in result['ResultSet'].keys():
            return result['ResultSet']['Rows']
    return []


def wait4Query(qid):
    while queryStatus(qid) in ['QUEUED', 'RUNNING']:
        print("Waiting query id to finish {}".format(qid))
        sleep(5)
    print("query id finished {}".format(qid))
    return qid


def run_query(query):
    print("Running query: {}".format(query))
    qid = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': dbName},
        ResultConfiguration={'OutputLocation': oputBucket}
    )['QueryExecutionId']
    print("query id: {}".format(qid))
    return qid


def handler(event, context):
    print(json.dumps(event))
    epocRegex = r'\t(\d.+)$'

    try:
        query = 'SHOW TABLES;'
        tableResults = getQueryResult(wait4Query(run_query(query)))
        for tableResult in tableResults:
            table = tableResult['Data'][0]['VarCharValue']
            print("Table: {}".format(table))
            query = "SHOW TBLPROPERTIES {}('transient_lastDdlTime');".format(table)
            property = getQueryResult(wait4Query(run_query(query)))[0]['Data'][0]['VarCharValue']

            tableCreatedAt = re.findall(epocRegex, property)
            if tableCreatedAt:
                print("Table: {} created at: {}".format(table, tableCreatedAt[0]))
                if (datetime.utcnow() - datetime.utcfromtimestamp(int(tableCreatedAt[0]))).days > 30:
                    print("Table: {} older then 30 days".format(table))
                    print("Deleting Table: {}".format(table))
                    run_query("DROP TABLE IF EXISTS {};".format(table))
                    query = "SHOW VIEWS LIKE '{}*_view';".format(table)
                    for views in getQueryResult(wait4Query(run_query(query))):
                        print("Deleting Views: {}".format(views['Data'][0]['VarCharValue']))
                        run_query("DROP VIEW IF EXISTS {};".format(views['Data'][0]['VarCharValue']))
    except Exception as e:
        print(str(e))
        traceback.print_exc()
    return 0
