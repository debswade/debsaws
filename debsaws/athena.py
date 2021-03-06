from __future__ import print_function
from debsaws.common import base_operating_premise
from time import sleep

region, instanceid, basedir, session = base_operating_premise()

client = session.client('athena')

def run_query(query, athenadb, s3loc):
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': athenadb
            },
        ResultConfiguration={
            'OutputLocation': s3loc,
            }
        )
    # print('Execution ID: ' + response['QueryExecutionId'])
    return response

def results_to_df(results):
    columns = [
        col['Label']
        for col in results['ResultSet']['ResultSetMetadata']['ColumnInfo']
    ]
    listed_results = []
    for res in results['ResultSet']['Rows'][1:]:
        values = []
        for field in res['Data']:
            try:
                values.append(list(field.values())[0])
            except:
                values.append(list(' '))
        listed_results.append(dict(zip(columns, values)))
    return listed_results

def get_status(QueryExecutionId):
    response = client.get_query_execution(QueryExecutionId = QueryExecutionId)
    return response


def get_query_results(executionid):
    jobstatus = get_status(executionid)
    state = jobstatus['QueryExecution']['Status']['State']
    # 'State': 'QUEUED' | 'RUNNING' | 'SUCCEEDED' | 'FAILED' | 'CANCELLED',
    while state == 'RUNNING':
        sleep(3)
        jobstatus = get_status(executionid)
        state = jobstatus[ 'QueryExecution' ][ 'Status' ][ 'State' ]

    if state == 'FAILED':
        fail_reason = jobstatus[ 'QueryExecution' ][ 'Status' ][ 'StateChangeReason' ]
        return state, fail_reason

    elif state =='SUCCEEDED':
        state = jobstatus[ 'QueryExecution' ][ 'Status' ][ 'State' ]
        s3location = jobstatus[ 'QueryExecution' ][ 'ResultConfiguration' ][ 'OutputLocation' ]
        print(f' Wanna download: {s3location}')
        return state,s3location
