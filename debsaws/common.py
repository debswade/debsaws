import boto3
import datetime
from socket import gethostname
from pathlib import Path
from os import getcwd
from ec2_metadata import ec2_metadata
import csv
from debsaws.config import dbuser

hostname = gethostname()
sshusername = "debsbalm"
sshpkey = "~/.ssh/debsbalm.pub"

project = "cb-prod-uk-athena"
account = 'boilerdiag-prod'
athenadb = 'biq_prod'
result_dir = 's3_results'
source_table = 'cb-prod-bosch-parameters'
dest_table = 'cb-prod-params-poc'
bucket_name = 'cb-prod-uk-athena-query-results'
dummy_instanceid = 'i-0d88633bbd7362686'

nowish = datetime.date.today()
env = account
secret_name = f"{account}/rds-users/{dbuser}"
s3loc = f's3://{bucket_name}/visualisation/'

def base_operating_premise():
    if 'compute.internal' in hostname:
        instanceid = ec2_metadata.instance_id
        region = ec2_metadata.region
        basedir = f'/home/ec2-user/{project}'
        session = boto3.Session(region_name=region_name)
    else:
        instanceid = dummy_instanceid
        region = 'eu-west-1'
        basedir = getcwd()
        session = boto3.Session(profile_name=account)
    return region, instanceid, basedir, session

region, instanceid, basedir, session = base_operating_premise()

measurement_labels = {
    'PrimT': 'PT',
	'FanRpm': 'FR',
	'FlameCur': 'FC',
	'HwTOutlet': 'HO',
	'FanRpmSet': 'FS',
	'ActPow': 'AP',
	'Undefined': 'XX'
}


def split_s3_loc(s3_loc):
	path_parts = s3_loc.replace("s3://","").split("/")
	bucket = path_parts.pop(0)
	key = "/".join(path_parts)
	file = path_parts.pop()
	return bucket,key,file


def download_s3_file(filepath,calling_script):
	s3_client = session.client('s3')

	bucket,key,file = split_s3_loc(filepath)
	target_file = f'{basedir}/{calling_script}/{file}'

	print(f'DOWNLOADING: {key} from {bucket} to {target_file}')
	response = s3_client.download_file(bucket,key,target_file)
	# print (response)

	return target_file


def read_data_from_file(datafile):
	datafile = Path(f'{datafile}')
	if datafile.is_file():
		with open(datafile, 'r') as h:
			next(h)
			csv_reader = csv.reader(h)
			lines = list(csv_reader)
	return lines


def get_midnight_timestamp(tsdate):
	dtts = datetime.datetime.utcfromtimestamp(tsdate)

	dt = datetime.datetime.combine(dtts.date(),datetime.datetime.min.time())
	mnts = int(datetime.datetime.timestamp(dt))
	# print(mnts)
	return mnts


def get_relative_timestamps(start_offset,end_offset):
	# Returns a start and end timestamp for a given offset from today
	utcnow = datetime.datetime.utcnow().timestamp()
	# print(utcnow)
	dayseconds = (24 * 60 * 60)
	start = get_midnight_timestamp(utcnow - (start_offset * dayseconds))
	end = get_midnight_timestamp(utcnow - ((end_offset - 1) * dayseconds))

	return start,end


def get_lasthour_timestamps(hour_offset):
	hour_offset = hour_offset + 1
	# Returns a start and end timestamp for previous hour minus any offset
	hournow = datetime.datetime.now().replace(microsecond=0,second=0,minute=0)
	# print(hournow)
	start = hournow - datetime.timedelta(hours=hour_offset)
	end = hournow
	return start,end
