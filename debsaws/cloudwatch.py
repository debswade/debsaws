import datetime
from common import base_operating_premise

region, instanceid, basedir, session = base_operating_premise()

startTime = datetime.datetime.now()
nowish = datetime.date.today()
cloudwatch_client = session.client('cloudwatch')


def get_cloudwatch_lambda_metric(metric_name, function_name, period, start_ts, end_ts):
    response = cloudwatch_client.get_metric_statistics(
        Namespace='AWS/Lambda',
        Dimensions=[
            {
                'Name': 'FunctionName',
                'Value': function_name
            },
        ],
        MetricName=metric_name,
        StartTime = start_ts,
        EndTime = end_ts,
        Period = period,
        Statistics=['Maximum'],
        Unit='Milliseconds'
    )

    return response

