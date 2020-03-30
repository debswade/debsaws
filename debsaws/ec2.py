import datetime
from common import base_operating_premise

region, instanceid, basedir, session = base_operating_premise()
ec2_client = session.client('ec2')
startTime = datetime.datetime.now()
nowish = datetime.date.today()

def make_filter(state=None, stackname=None):
    filters = []
    if state is not None:
        filter = {'Name': 'instance-state-name', 'Values': [ state ]}
        filters.append(filter)

    if stackname is not None:
        filter = {'Name': 'tag-key', 'Values': [ 'aws:cloudformation:stack-name' ]}
        filters.append(filter)
        filter = {'Name': 'tag-value', 'Values': [ stackname ]}
        filters.append(filter)
    return filters

def lookup_instanceid_from_hostname(hostname):
    filters = [ {'Name': 'private-dns-name',
                'Values': [ hostname ]} ]
    response = ec2_client.describe_instances(Filters=filters)["Reservations"]
    instanceid = response[0]['Instances'][0]['InstanceId']
    return instanceid

def lookup_hostname_from_instanceid(instanceid):
    instanceids = [instanceid]
    response = ec2_client.describe_instances(InstanceIds=instanceids)
    print(response)
    hostname = response[0]['Instances'][0]['PrivateDnsName']
    return hostname

def get_running_instanceids_in_stack(stackname=None):
    filters = make_filter(state='running',stackname=stackname)
    response = ec2_client.describe_instances(Filters=filters)["Reservations"]
    instanceids = [i['InstanceId'] for r in response for i in r[ "Instances" ] ]
    return instanceids

def get_instances_by_state(state=None, stackname=None):
    # Return ec2 instances in the desired state
    # Valid Values: pending | running | shutting - down | terminated | stopping | stopped
    filters = make_filter(state=state, stackname=stackname)
    response = ec2_client.describe_instances(Filters=filters)[ "Reservations" ]
    return response
