import boto3
import json
import logging
import os
import hc_common.constants as const
import hc_common.handler_utils as handler
from botocore.exceptions import ClientError
from common import base_operating_premise

region, instanceid, basedir, session = base_operating_premise()
startTime = datetime.datetime.now()
nowish = datetime.date.today()

iot = session.client('iot-data')

logger = logging.getLogger()
logger.setLevel(handler.get_log_level_from_env())


def shadow(name, metadata=True, client=iot):
    try:
        print('in shadow fetch')

        data = client.get_thing_shadow(thingName=name)['payload'].read()
        # We are only interested in state.reported part. Everything else can be ignored
        data_json = {'state': {'reported': json.loads(data)['state']['reported']}}

        if not metadata:
            data_json['state']['reported'].pop('metadata', None)

        return data_json

    except ClientError as e:
        if e.response['Error']['Code'] != 'ResourceNotFoundException':
            logger.error('Error when looking for shadow [{}]: {}'.format(name, e))
        return None

    except KeyError as e:
        logger.error('KeyError: {}'.format(str(e)))
        return None


def delete_things(device_shadows, iot_client=boto3.client('iot'), iot_data_client=boto3.client('iot-data')):
    for k, v in device_shadows.iteritems():
        # Currently the system does not create a thing, only shadows.
        # Since a thing can be created using other sources, its deletion should be invoked.
        iot_client.delete_thing(thingName=v)

        # statement above deletes a thing only but leaves its shadows.
        try:
            iot_data_client.delete_thing_shadow(thingName=v)
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                # Shadows may not exist for the given thing yet. In such case, we want to proceed with device deletion
                continue
            raise e


def update_camera_state(node, payload):
    if payload:
        publish(
            topic='{}/device/status/{}/{}'.format(os.environ['ENVIRONMENT'], const.DEVICE_TYPE_BRIDGE_CAMERA, node),
            payload=payload
        )


def update_settings_leak(node, payload):
    if payload:
        publish(
            topic='{}/device/status/{}/{}/decoded'.format(
                os.environ['ENVIRONMENT'],
                const.DEVICE_TYPE_BRIDGE_LEAK,
                node),
            payload=payload
        )


def publish(topic, payload=None):
    logger.debug('Publishing to IoT: topic={} | payload={}'.format(topic, json.dumps(payload)))
    iot.publish(topic=topic, qos=1, payload=json.dumps(payload if payload else {}))


def shadow_ids(node):
    honeycomb_internal_feature = shadow('{}_{}'.format(os.environ['ENVIRONMENT'], node))
    try:
        return honeycomb_internal_feature['state']['reported']['ShadowIds']

    except (AttributeError, KeyError, TypeError):
        return {}


def update_shadow(thing, payload):
    if payload:
        publish(
            topic='$aws/things/{}/shadow/update'.format(thing),
            payload=payload
        )


def notification_topic(notification, node, device_type):
    publish(
        topic='{}/device/event/{}/{}/{}'.format(os.environ['ENVIRONMENT'], device_type, notification, node)
    )
