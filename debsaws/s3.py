import re
import json
from common import base_operating_premise
from config import account, s3loc

region, instanceid, basedir, session = base_operating_premise()

desired_env = account
devbucket = 'honeycomb-node-registry-dev'
prodbucket = 'honeycomb-node-registry-prod'

bucket = s3loc
s3_client = session.client('s3')

def iterate_all_bucket_items(bucket, desired_env):
    cameras = []
    paginator = s3_client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket,
                                       # PaginationConfig={'MaxItems': 100}
                                       )

    for page in page_iterator:
        if page['KeyCount'] > 0:
            for item in page['Contents']:
                key = item['Key'].split('/')[0]
                p = re.compile('[A-Z0-9]{30}')
                m = p.match(key)

                if m:
                    keyreservation = item[ 'Key' ]
                    result = s3_client.get_object(Bucket=bucket,Key=keyreservation)
                    text = json.loads(result[ "Body" ].read().decode())
                    node=text["node"]

                    if 'environment' in text:
                        env = text[ "environment" ]
                        if env == desired_env:
                            # print(f'NODE: {node}, ENV: {env}, TYPE: {type}')
                            # key_env = (key, env)
                            cameras.append(key)

    return(cameras)