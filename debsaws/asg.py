from common import base_operating_premise


region, instanceid, basedir, session = base_operating_premise()
asg_client = session.client("autoscaling")


def get_asg_detail(autoscalingroup):
    response = asg_client.describe_auto_scaling_groups(AutoScalingGroupNames=[autoscalingroup])
    return response

def get_asg_instances(autoscalingroup):
    # filters = [ {'Name': 'key','Values': [ autoscalingroup ]} ]
    instanceids =[]
    response = asg_client.describe_auto_scaling_groups(
        AutoScalingGroupNames=[ autoscalingroup ])
    for group in response[ "AutoScalingGroups" ]:
        for instance in group["Instances"]:
            instanceinfo = {'InstanceId': instance['InstanceId'],
                            'LifecycleState' :instance["LifecycleState"],
                            'HealthStatus' :instance["HealthStatus"] }
            print(instanceinfo)
            instanceids.append(instanceinfo)
    return instanceids
