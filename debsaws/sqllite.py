import sqlite3
import os

# Designed for a local sqllite install

# DEFAULT_PATH = os.path.join(os.path.dirname(__file__), 'nodes_backup.sq3')
DEFAULT_PATH = os.path.join(os.path.dirname(__file__), 'nodesdb.sqlite3')

def db_connect(db_path=DEFAULT_PATH):
    con = sqlite3.connect(db_path)
    return con

# hubId, nodeId, homeId, uuid, userUuid, macAddress

def drop_table(table_name):
    con = db_connect()
    cur = con.cursor()
    nodescr_sql = f"DROP TABLE {table_name} "
    cur.execute(nodescr_sql)

def create_nodes(table_name):
    con = db_connect()
    cur = con.cursor()

    # nodescr_sql = f"DROP TABLE {table_name} "
    # cur.execute(nodescr_sql)

    nodescr_sql = f"CREATE TABLE {table_name} ( " \
                f"hubId integer , " \
                f"nodeId text , " \
                f"homeId text, " \
                f"uuid text PRIMARY KEY, " \
                f"userUuid text, " \
                f"macAddress  ) "
    cur.execute(nodescr_sql)

def drop_indexes():
    con = db_connect()
    cur = con.cursor()
    index_sql = f"DROP INDEX IF EXISTS hubId_idx)"
    cur.execute(index_sql)
    index_sql = f"DROP INDEX IF EXISTS homeId_idx)"
    cur.execute(index_sql)

def create_indexes(table_name):
    con = db_connect()
    cur = con.cursor()
    # index_sql = f"CREATE INDEX hubId_idx ON {table_name}(hubId)"
    # cur.execute(index_sql)
    # index_sql = f"CREATE INDEX homeId_idx ON {table_name}(homeId)"
    # cur.execute(index_sql)
    index_sql = f"CREATE INDEX nodeId_idx ON {table_name}(nodeId)"
    cur.execute(index_sql)

def insert_nodes():
    con = db_connect()
    cur = con.cursor()
    nodes_sql = "INSERT INTO nodes VALUES (?, ?, ?, ?, ?, ?)"


    cur.execute(nodes_sql, (2029728183196057601,
                            '::20d:6f00:b46:76c1',
                            'b3a53fca-4ef8-4976-bf91-1bf78920d993',
                            '9b56cec2-4b6f-4227-bb7b-cd6edfbad5a2',
                            '38ba5ca1-e178-4fc4-af86-d794c402b2cb',
                            '000D6F000B4676C1'))

    noderowid = cur.lastrowid
    print(f"noderowid {noderowid}")
    con.commit()

def chunks(seq, size):
    return (seq[i::size] for i in range(size))

def bulk_insert_nodes_dict(nodeslist):
    con = db_connect()
    # print(f"nodeslist: {nodeslist}")
    # print(f"nodeslist0: {nodeslist[0]}")

    nodes_sql = f"INSERT INTO nodes (hubId, nodeId, homeId, uuid, userUuid, macAddress)" \
                f" VALUES (?, ?, ?, ?, ?, ?)"

    chunkednodes = chunks(nodeslist,500)
    for nodes in chunkednodes:
        nodesnumber = 0
        print(f"nodes length {len(nodes)}")
        # print(f"nodes {nodes}")
        for nodesrow in nodes:
            # print(f"Nodesrow: {nodesrow}")

            con.execute('begin')
            cur = con.cursor()
            row = [
                nodesrow.get('hubId'),
                nodesrow.get('nodeId'),
                nodesrow.get('homeId'),
                nodesrow.get('uuid'),
                nodesrow.get('userUuid'),
                nodesrow.get('macAddress')  ]

            cur.execute(nodes_sql, row)
            nodesnumber = nodesnumber+1
            print(f"NodesBatchNumber: {nodesnumber}")
            noderowid = cur.lastrowid
            print(f"noderowid {noderowid}")

            con.commit()



def select_nodes():
    con = db_connect()
    cur = con.cursor()
    nodessel_sql = "SELECT hubId,nodeId,homeId,uuid,userUuid,macAddress FROM nodes limit 10"
    cur.execute(nodessel_sql)
    result = cur.fetchall()
    return result

def nodes_gaps(sourceNode):
    con = db_connect()
    cur = con.cursor()
    nodessel_sql = f"SELECT homeId,uuid FROM nodes WHERE nodeId = '{sourceNode}' " \
                   f"ORDER by uuid desc Limit 1 "
    # nodessel_sql = f"SELECT hubId,nodeId,homeId,uuid,userUuid,macAddress FROM nodes WHERE nodeId = {sourceNode} "
    cur.execute(nodessel_sql)
    result = cur.fetchall()
    return result

def get_recs_for_home(homeid):
    con = db_connect()
    cur = con.cursor()
    nodessel_sql = f"SELECT hubId,nodeId,homeId,uuid,userUuid,macAddress FROM nodes WHERE homeId = '{homeid}' "
    # nodessel_sql = f"SELECT hubId,nodeId,homeId,uuid,userUuid,macAddress FROM nodes WHERE nodeId = {sourceNode} "
    cur.execute(nodessel_sql)
    result = cur.fetchall()
    # foundrecs = cur.arraysize
    # print(f"Found: {foundrecs}")
    return result

def get_recs_for_hubid(hubid):
    con = db_connect()
    cur = con.cursor()
    nodessel_sql = f"SELECT hubId,nodeId,homeId,uuid,userUuid,macAddress FROM nodes WHERE hubId = '{hubid}' "
    # nodessel_sql = f"SELECT hubId,nodeId,homeId,uuid,userUuid,macAddress FROM nodes WHERE nodeId = {sourceNode} "
    cur.execute(nodessel_sql)
    result = cur.fetchall()
    # foundrecs = cur.arraysize
    # print(f"Found: {foundrecs}")
    return result
def get_recs_for_node(nodeid):
    con = db_connect()
    cur = con.cursor()
    nodessel_sql = f"SELECT hubId,nodeId,homeId,uuid,userUuid,macAddress " \
                   f"FROM nodes WHERE nodeId = '{nodeid}'" \
                   f"ORDER by uuid desc Limit 1 "
    # nodessel_sql = f"SELECT hubId,nodeId,homeId,uuid,userUuid,macAddress FROM nodes WHERE nodeId = {sourceNode} "
    cur.execute(nodessel_sql)
    result = cur.fetchall()
    # foundrecs = cur.arraysize
    # print(f"Found: {foundrecs}")
    return result

def desc_tables():
    con = db_connect()
    cur = con.cursor()
    cur.execute("SELECT sql FROM sqlite_master WHERE type='table'")
    print(cur.fetchall())


# def go():
#     # drop_table('nodes')
#     create = create_nodes()
#     print (f"Create output : {create}")
#     desc_tables()
#
#     ins = insert_nodes()
#     sel = select_nodes()
#     print (f"Select output : {sel}")
#     # for row in select:
#     #     print(row)
# go()
