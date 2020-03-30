import boto3
import time
import datetime
from common import base_operating_premise

region, instanceid, basedir, session = base_operating_premise()

startTime = datetime.datetime.now()
nowish = datetime.date.today()
ssm_client = session.client('ssm')
s3_client = session.client('s3')
sns_client = session.client('sns')


def get_instances_with_command_status(check_instances, status):
    idsinstatus = get_commands_with_status(status)
    statusinstances = []
    for x in idsinstatus:
        for i in x['InstanceIds']:
            statusinstances.append(i)
    notstatusinstances = [item for item in check_instances if item not in statusinstances]
    return statusinstances, notstatusinstances

def get_running_processes_named_status(status, invokedafter):

    response = ssm_client.list_commands(Filters=[
                            {   'key': 'Status',
                                'value' : status },
                            {'key': 'InvokedAfter',
                             'value': invokedafter}
                            ] )
    print(len(response[ 'Commands' ]))

    return response


def get_running_instances_named_status(instance, status):
    response = ssm_client.list_commands(InstanceId=instance,
                                        Filters=[{'key': 'Status', 'value': status}]
                                        )
    return response


def get_commands_with_status(status):
    response = ssm_client.list_commands(Filters=[{'key': 'Status', 'value': status}]
                                        )
    commands = response['Commands']
    return commands



def get_running_command_detail():
    status ='InProgress'
    response = ssm_client.list_commands(Filters=[{'key': 'Status', 'value': status}]
                                        )
    return response


def get_idle_instances(check_instances):
    status = 'InProgress'
    working = get_commands_with_status(status)
    runninginstances = []
    for x in working:
        for i in x['InstanceIds']:
            runninginstances.append(i)
    # print(f"Running {runninginstances}")
    idle_instances = [item for item in check_instances if item not in runninginstances]
    print(f"Idle {idle_instances}")
    return idle_instances


def command_instance_latest(instance, status, invokedafter):
    # print(instance)
    response = ssm_client.list_commands(
        InstanceId=instance,
        Filters=[{'key': 'ExecutionStage',
                'value': status        },
                {'key': 'InvokedAfter',
                 'value': invokedafter }
                 ],
    )
    return response


def start_job_on_mig_instance(instances, linux_cmd, batchno):
    # linux_cmd = 'python3 do_migrate.py 1546300800 prod 400'

    cloudwatchloggroup=f"TSDBMig{batchno}"

    response = ssm_client.send_command(
        InstanceIds=instances,
        DocumentName="AWS-RunShellScript",
        Comment=f"Autostart Migration {batchno} ",
        Parameters={ "workingDirectory": ["/home/ec2-user/tsdb-new-migration"],
                     "executionTimeout": ["172800"],
                     "commands": [linux_cmd] },
        ServiceRoleArn = "arn:aws:iam::806125994151:role/ts-migrationsnsrole",
        NotificationConfig={ "NotificationArn": "arn:aws:sns:eu-west-1:806125994151:ts-migration",
                             "NotificationEvents": ["All"],
                             "NotificationType": "Command"   },
        CloudWatchOutputConfig={
            'CloudWatchLogGroupName': cloudwatchloggroup,
            'CloudWatchOutputEnabled': True
        }
    )
    command_id = response[ 'Command' ][ 'CommandId' ]
    return command_id

def send_command(instance,linux_cmd):
    print(instance)
    inst = instance[0]
    response = ssm_client.send_command(
        InstanceIds=instance,
        DocumentName="AWS-RunShellScript",
        Parameters={
            'commands': [ linux_cmd ]
        },
    )
    command_id = response[ 'Command' ][ 'CommandId' ]
    return command_id

def cancel_command(commandid, instanceids):
    response = ssm_client.cancel_command(
        CommandId = commandid,
        InstanceIds=instanceids  )
    return response

def get_cmd_outcome(command_id,instance):
    output = ssm_client.get_command_invocation(
        CommandId=command_id,
        InstanceId=instance,
    )
    # print(output)
    return output

def check_all_outcomes(commandids):
    # print(commandids)
    statuses = [ ]
    status_detail = [ ]
    for command_id,instance in commandids:
        # print( command_id, instance)
        outcomes = get_cmd_outcome(command_id,instance)
        # print(outcomes['Status'], instance)
        # print(outcomes[ 'StandardOutputContent' ], instance)

        jobstatus = outcomes[ 'Status' ]
        # print(f'Instance {instance} Jobstatus: {jobstatus}')
        status_detail.append((instance,jobstatus))
        statuses.append(jobstatus)

    return statuses,status_detail


def run_commands_on_instances(instances, linux_cmd):
    commandids = []
    for instance in instances:
        # print(linux_cmd)
        run_linux_cmd = linux_cmd.replace('INSTANCE', instance, 10)
        print(f'{instance} commanded with {run_linux_cmd}')
        instlist = [instance]
        command_id = send_command(instlist,run_linux_cmd)
        command_inst = (command_id,instance)
        commandids.append(command_inst)

    print('Sleeping for 10')
    time.sleep(10)

        # Wait whilst status of each job is Completed
    stats = [ ]
    while 'InProgress' in stats or len(stats) <= 0:
        stats,stats_deets = check_all_outcomes(commandids)
        inpro = [ x[ 0 ] for x in stats_deets if 'InProgress' in x[ 1 ] ]
        print(f'RUNNING Loop awaiting {len(inpro)} instances ({inpro}).')
        print(f'Sleep for {(5*inpro)}')
        time.sleep((5 * len(inpro)))

    return stats_deets
