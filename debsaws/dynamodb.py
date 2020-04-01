import json
import queue
import ast
import decimal
import threading
import datetime
from debsaws.common import base_operating_premise
from boto3.dynamodb.conditions import Key, Attr

region, instanceid, basedir, session = base_operating_premise()
ec2_client = session.client('ec2')
startTime = datetime.datetime.now()
nowish = datetime.date.today()

dynamodb_client = session.client("dynamodb")
dynamodb_resource = session.resource('dynamodb')

def list_tables(account):
    tables = dynamodb_client.list_tables()
    return tables

def describe_table(table):
    response = dynamodb_client.describe_table(
        TableName=table    )
    return response

def get_table_metadata(account, table_name):
    table = dynamodb_resource.Table(table_name)

    return {
        'table_name': table.table_name,
        'table_arn': table.table_arn,
        'num_items': table.item_count,
        'created': table.creation_date_time,
        'table_throughput': table.provisioned_throughput,
        'primary_key_name': table.key_schema[0],
        'status': table.table_status,
        'bytes_size': table.table_size_bytes,
        'global_secondary_indices': table.global_secondary_indexes,
        'table_attributes': table.attribute_definitions,
    }


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)

def query_all_rows(account, table_name):
    table = dynamodb_resource.Table(table_name)
    response = table.scan()
    data = response[ 'Items' ]

    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response[ 'LastEvaluatedKey' ])
        data.extend(response[ 'Items' ])

    return data

def query_some_rows(account, table_name):
    table = dynamodb_resource.Table(table_name)
    response = table.scan()
    data = response[ 'Items' ]

    rows = []
    for i in data:
        d = ast.literal_eval((json.dumps(i,cls=DecimalEncoder)))
        rows.append(d)
    return rows

def delete_item(table, Item):
    dynamodb_table = dynamodb_resource.Table(table)
    response = dynamodb_table.delete_item(Key = Item )
    return response

def query_item(table, Item):
    dynamodb_table = dynamodb_resource.Table(table)
    response = dynamodb_table.get_item(Key = Item )
    return response

def scan_hub_items(table, hubuuid):
    dynamodb_table = dynamodb_resource.Table(table)
    response = dynamodb_table.scan( FilterExpression=Attr('pk').eq(hubuuid) &
                                       Attr('sk').between(1560294000, 1560340800 ) )
    data = response[ 'Items' ]
    while 'LastEvaluatedKey' in response:
        response = dynamodb_table.scan(FilterExpression=Attr('pk').eq(hubuuid) &
                                       Attr('sk').between(1560294000, 1560340800),
                                       ExclusiveStartKey=response[ 'LastEvaluatedKey' ])
        data.extend(response[ 'Items' ])
    rows = [ ]
    for i in data:
        d = ast.literal_eval((json.dumps(i,cls=DecimalEncoder)))
        rows.append(d)
    return response


def scan_table_lite(table, filterexpression, limit):
    dynamodb_table = dynamodb_resource.Table(table)
    # print(f"FilterExpression {filterexpression}")
    # print(f"Table {table}")
    response = dynamodb_table.scan( FilterExpression=eval(filterexpression),
                                    Limit = limit)

    return response

def scan_order_table(table, filterexpression, limit):
    dynamodb_table = dynamodb_resource.Table(table)
    # print(f"FilterExpression {filterexpression}")
    response = dynamodb_table.scan( FilterExpression=eval(filterexpression),
                                    ScanIndexForward=False,
                                    Limit = limit)

    return response

def scan_table(table, filterexpression):
    dynamodb_table = dynamodb_resource.Table(table)
    print(f"FilterExpression {filterexpression}")
    response = dynamodb_table.scan( FilterExpression=eval(filterexpression))
    data = response[ 'Items' ]
    while 'LastEvaluatedKey' in response:
        response = dynamodb_table.scan(FilterExpression=eval(filterexpression),
                                       ExclusiveStartKey=response[ 'LastEvaluatedKey' ])
        data.extend(response[ 'Items' ])

    # rows = [ ]
    # for i in data:
    #     d = ast.literal_eval((json.dumps(i,cls=DecimalEncoder)))
    #     rows.append(d)
    return data


def item_exists(dest_table, uuid, new_store_start_ts, new_store_end_ts):
    dynamodb_table = dynamodb_resource.Table(dest_table)
    response = dynamodb_table.query( KeyConditionExpression=Key('pk').eq(uuid) &
                                       Key('sk').between(new_store_start_ts, new_store_end_ts ),
                                     ProjectionExpression= "pk, sk" )
    data = response[ 'Items' ]
    if len(data) > 0 :
        exists = 'Y'
    else:
        exists = 'N'
    return exists

def query_hub_items_between(table, uuid, date1, date2):
    dynamodb_table = dynamodb_resource.Table(table)
    # print (f"UUID: {uuid}, SK:{date1} - {date2}, TABLE: {table}")
    response = dynamodb_table.query( KeyConditionExpression=Key('pk').eq(uuid) &
                                       Key('sk').between(date1, date2 ) )
    data = response[ 'Items' ]
    while 'LastEvaluatedKey' in response:
        response = dynamodb_table.query( KeyConditionExpression=Key('pk').eq(uuid) &
                                       Key('sk').between(date1, date2),
                                       ExclusiveStartKey=response[ 'LastEvaluatedKey' ])
        data.extend(response[ 'Items' ])

    return data

def _batch(iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]

def insert_item(table, Item):
    dynamodb_table = dynamodb_resource.Table(table)
    response = dynamodb_table.put_item(Item=Item)
    return response

def put_dynamodb(items, table):
    batches = _batch(items, 25)
    for batch in batches:
        # print(batch)
        response = batch_put(batch,table)
        if response is not True: print(response)
    return response

def put_dynamodb_threaded(table, items):
    batches = _batch(items, 25)
    threads_to_start =50
    my_queue = queue.Queue()

    def worker():
        while not my_queue.empty():
            data = my_queue.get()
            batch_put(table, data)
            my_queue.task_done()
            # print(f"Q: {my_queue.qsize()}")

    for batch in batches:
        my_queue.put(batch)

    print(f"Start queue length: {my_queue.qsize()}")

    for i in range(threads_to_start):
        t = threading.Thread(target=worker,
                             daemon=True)  # daemon means that all threads will exit when the main thread exits
        t.start()

    my_queue.join()



def batch_put(table, Items):
    filepath = f"{basedir}/fails.txt"
    dynamodb_table = dynamodb_resource.Table(table)
    with dynamodb_table.batch_writer() as batch:
        # print(type(Items))
        for item in Items:
            # print(item)
            try:
                x = batch.put_item(Item=item)
            except:
                try:
                    insert_item(table, item)
                except Exception as r:
                    err = f"EXCEPTION {r} Cant put {item}"
                    print(err)

                    with open(filepath,'a') as outfile:
                        outfile.write(f"{item}\n {r}\n")
    return len(Items)

def batch_delete(table,Items):
    dynamodb_table = dynamodb_resource.Table(table)
    with dynamodb_table.batch_writer() as batch:
        for item in Items:
            batch.delete_item(item)

def update_items(table,item):
    dynamodb_table = dynamodb_resource.Table(table)
    update = dynamodb_table.put_item(Item=item)
    return update

def delete_item(table, Item):
    dynamodb_table = dynamodb_resource.Table(table)
    response = dynamodb_table.delete_item(Key = Item )
    return response