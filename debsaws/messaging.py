import boto3
import datetime
from common import base_operating_premise, test_table, runner_table
from config import hostname, account, s3loc
region, instanceid, basedir, session = base_operating_premise()
migdir = basedir
bucket = s3loc
nowish = datetime.date.today()

ssm_client = session.client('ssm')
sns_client = session.client('sns')
sqs_client = session.client('sqs')
sts_client = session.client('sts')

sqs_queue_url = 'https://sqs.eu-west-1.amazonaws.com/806125994151/DebsTestQueue'
# sqs_queue_url = 'https://sqs.eu-west-1.amazonaws.com/806125994151/ts-store-eu-west-1-DataDumpQueue-1OR4R4E2SE4RG'

def sns_signal(number, message):
    response = sns_client.publish(PhoneNumber=number,Message=message )
    return response

def sqs_send(message):
    #queue = sqs_client.get_queue_by_name(QueueName='DebsTestQueue')
    response = sqs_client.send_message(
        QueueUrl=sqs_queue_url,
        MessageBody=message,
    )
    return response

def sqs_batch_send(message_batch):
    #queue = sqs_client.get_queue_by_name(QueueName='DebsTestQueue')

    response = sqs_client.send_message_batch(
        QueueUrl=sqs_queue_url,
        Entries=message_batch,
    )
    return response

def sqs_receive():
    # queue = sqs_client.get_queue_by_name(QueueName='test')

    response = sqs_client.receive_message(
        QueueUrl=sqs_queue_url,
        MaxNumberOfMessages=1,
        MessageAttributeNames=['All']
    )

    message = response[ 'Messages' ][ 0 ]
    # receipt_handle = message[ 'ReceiptHandle' ]
    # msg_id = message[ 'MessageId' ]
    msg_body = message[ 'Body' ]
    msg_receipt = message[ 'ReceiptHandle' ]

    return msg_receipt, msg_body

def sqs_delete(msg_receipt):
    # queue = sqs_client.get_queue_by_name(QueueName='test')

    response = sqs_client.delete_message(
        QueueUrl=sqs_queue_url,
        ReceiptHandle=msg_receipt
    )

    print('Received and deleted message: %s' % msg_receipt)


def get_cloudwatch_lambda_metric(metric_name, function_name, period, start_ts, end_ts):
    if 'compute.internal' in hostname:
        cloudwatch_client = assume_role_client()
    else:
        cloudwatch_session = boto3.Session(profile_name=account)
        cloudwatch_client = cloudwatch_session.client('cloudwatch')

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


def assume_role_client():
    sts = session.client('sts')
    assume_role_response = sts.assume_role(
        RoleArn='arn:aws:iam::806125994151:role/ts-store-eu-west-1-migration-stream',
        RoleSessionName="AssumeRoleSession1")

    credentials = assume_role_response[ 'Credentials' ]

    return session.client(
        'cloudwatch',
        aws_access_key_id=credentials[ 'AccessKeyId' ],
        aws_secret_access_key=credentials[ 'SecretAccessKey' ],
        aws_session_token=credentials[ 'SessionToken' ],
        region_name='eu-west-1'
    )

