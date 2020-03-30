import pymysql
import json
from botocore.exceptions import ClientError
import config
from sshtunnel import SSHTunnelForwarder
import paramiko
import datetime
from common import base_operating_premise
from config import hostname, dbuser, secret_name


region, instanceid, basedir, session = base_operating_premise()
migdir = basedir
ec2_client = session.client('ec2')
startTime = datetime.datetime.now()
nowish = datetime.date.today()


def get_creds(user, secret_name):
    try:
        secretjson = {'username': config.username, 'password': config.password}
        return secretjson
    except ClientError as p:
        secret_client = session.client("secretsmanager")
        try:
            secretjson = json.loads(secret_client.get_secret_value(SecretId=secret_name)['SecretString'])
            return secretjson
        except ClientError as e:
            if e.response[ 'Error' ][ 'Code' ] == 'ResourceNotFoundException':
                error = f'Can\'t find user {user}'
            else:
                error = f'Unexpected error: {e}'
            return error

def assign_db(db):
    sshusername = config.sshusername
    sshpkey = config.sshpkey
    mysqlport = 1233
    tunnel_name = ""
    if db == "events":
        dbhost = "eventlog-cluster.cluster-ro-cioh2pcuxrqc.eu-west-1.rds.amazonaws.com"
        # dbhost = "eventlog-cluster.cluster-cioh2pcuxrqc.eu-west-1.rds.amazonaws.com"
        mysqldb = "events"
    elif db == "ecs":
        dbhost = "mysql57-ecs.cm2nemvotkwb.eu-west-1.rds.amazonaws.com"
        mysqldb = "users"
        tunnel_name = 'ecs-tunnel'
    elif db == "ecsmig":
        dbhost = "ecs-restored-snapshot.cvra07b215pu.eu-west-1.rds.amazonaws.com"
        mysqldb = "users"
        tunnel_name = 'tsdb-ecs-tunnel'
        sshusername = config.tsdbsshusername
        sshpkey = config.tsdbsshpkey
    elif db == "tsdbrunner":
        dbhost = "tsdb-migration-rundb.cvra07b215pu.eu-west-1.rds.amazonaws.com"
        mysqldb = "nodes"
        tunnel_name = 'tsdb-mysql-tunnel'
        sshusername = config.tsdbsshusername
        sshpkey = config.tsdbsshpkey
    # print(f"{ dbhost} {mysqldb} {mysqlport} {tunnel_name} {sshusername} {sshpkey}")
    return dbhost, mysqldb, mysqlport, tunnel_name, sshusername, sshpkey

def query_rds(sql_command, user, db, data=None):
    # print(data)
    rows = make_connection(sql_command,user,db, data=data)
    # print(f"Query_rds returning rows {len(rows)}")
    return rows

def make_connection(sql_command, user, db, data=None):
    dbhost, mysqldb, mysqlport, tunnel_name, sshusername, sshpkey = assign_db(db)
    secretjson = get_creds(user, secret_name)
    mysqlpass = secretjson[ "password" ]
    mysqluser = secretjson[ "username" ]

    rows = []
    if 'compute.internal' in hostname:
        mysqlport = 3306
        db = pymysql.connect(host=dbhost,
                             user=mysqluser,
                             passwd=mysqlpass,
                             port=mysqlport,
                             db=mysqldb,
                             cursorclass=pymysql.cursors.DictCursor)
        with db:
            if "FOR UPDATE" in sql_command:
                # print(f"doing for update")
                rows = reserve_and_update_rows(sql_command, db)
            else:
                rows = exec_query(sql_command, db, data=data)

        db.close()
    else:
        mysqllocal = "127.0.0.1"
        with SSHTunnelForwarder(
            tunnel_name,
            ssh_username=sshusername,
            ssh_pkey=sshpkey,
            remote_bind_address=(dbhost, 3306),
            local_bind_address=(mysqllocal, mysqlport)
        ) as tunnel:
            client = paramiko.SSHClient()
            client.load_system_host_keys()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            db = pymysql.connect(host=mysqllocal,
                                 user=mysqluser,
                                 passwd=mysqlpass,
                                 port=mysqlport,
                                 db=mysqldb,
                                 cursorclass=pymysql.cursors.DictCursor)
            with db:
                rows = []
                # print(data)
                if "FOR UPDATE" in sql_command:
                    # print(f"doing for update with no extra data")
                    rows = reserve_and_update_rows(sql_command, db)
                else:
                    rows = exec_query(sql_command, db, data=data)
            db.close()
    # print(f'Make_connection returning rows {len(rows)}')
    return rows

def exec_query(sql_command, db, data=None):
    cur = db.cursor(pymysql.cursors.DictCursor)
    if data is None:
        # print(cur.mogrify(sql_command))
        cur.execute(sql_command)
    else:
        # print(cur.mogrify(sql_command))
        cur.executemany(sql_command,data)
    rows = cur.fetchall()
    db.commit()
    if rows is not None:
        return rows

def reserve_and_update_rows(sel_sql_command, db):
    # print(f'USER: {user} SQL: {sql_command} DB: {db}')
    cur = db.cursor(pymysql.cursors.DictCursor)
    # print(f"RESERVE: executing {cur.mogrify(sel_sql_command)}")
    cur.execute(sel_sql_command)
    rows = cur.fetchall()
    if len(rows) >0:
        # print(f"rows from rds.reserve_and_update_rows {rows}")
        migrator = instanceid
        migrated = "WORKING"
        uuids = [x['uuid'] for x in rows]
        # print(f"hubs {hubs}")
        uuidlist = ', '.join(f'"{h}"' for h in uuids)
        # print(f"uuidlist {uuidlist}")
        upd_sql = f'UPDATE users.nodedevices ' \
                  f'SET migrator = "{migrator}", ' \
                  f'migrated = "{migrated}" ' \
                  f'where uuid in ( {uuidlist} ) ' \
                  f'; '
        # print(upd_sql)
        # print(f"executing (reserve_and_update_rows) {cur.mogrify(upd_sql)}")
        cur.execute(upd_sql)
        db.commit()

    return rows
