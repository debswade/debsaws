from __future__ import print_function
from config import account, athenadb, s3loc
from common import session

client = session.client('athena')

def run_query(query):
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
