import os
sshusername = 'sshusername'
sshpkey = f'~/.ssh/{sshusername}.pub'
project = 'this_projectname'
account = 'aws_account'
dbuser = 'dbaadmin'
region = 'eu-west-1'
env = account
athenadb = 'athenadb'
bucket_name = 'bucket_name'
bucket_dir = 'bucket_dir'
master_bucket_name = 'master_bucket_name'
master_bucket_dir  = 'master_bucket_dir'
s3loc = f's3://{bucket_name}/visualisation/'
result_dir = 's3_results'
source_table = 'config-file-source-table'
dest_table = 'config-file-dest-table'
basedir = os.getcwd()