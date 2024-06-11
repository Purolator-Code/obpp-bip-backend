import json
import boto3
import os

def lambda_handler(event, context):
    ddb_client = boto3.client('dynamodb')
    query_params=event['queryStringParameters']
    db_table = os.getenv('DYNAMO_DB')
    s3_bucket_name = os.getenv('S3_BUCKET')
    otb_db_table = os.getenv('OTB_DYNAMO_DB')
    otb_s3_bucket = os.getenv('OTB_S3_BUCKET')

    invoice_number = parse_range(query_params.get('invoice_number', ''))
    customer_number = parse_range(query_params.get('customer_number', ''))
    is_otb = query_params.get('is_otb', '')
    order_number = parse_range(query_params.get('pin', ''))
    if (order_number == '') :
        order_number = parse_range(query_params.get('manifest'))
    data_complete = query_params.get('dc', '')
    manifest = query_params.get('manifest', '')
    start_date = int(query_params.get('start_date', 0))
    end_date = int(query_params.get('end_date', 99999999))

    if (is_otb == 'yes'):
        order_number = query_params.get('pin', '')
        if (order_number == '') :
            order_number = query_params.get('manifest')
        item = perform_query(otb_db_table, 'orders-order-number-index', order_number)
        url = "No Data Found"
        scan_id = ""
        if item is not None and 'scan_id' in item and item['scan_id'] is not None and 'S' in item['scan_id']:
            scan_id = item['scan_id']['S']
        if scan_id is not None and scan_id.strip() != "":
            url = generate_presigned_url(otb_s3_bucket, scan_id)
        respone = {
            "isBase64Encoded": False,
            "statusCode": 200,
            "statusDescription": "200 OK",
            "headers": {"Set-cookie": "cookies", "Content-Type": "application/json"},
            "body": url, 
        }
        return respone
    ddb_rows = []
    query_results = {}

    def query_and_collect(table_name, index_name, key_name, values, key):
        items = []
        for value in values:
            response = ddb_client.query(
                TableName=table_name,
                IndexName=index_name,
                KeyConditionExpression=f"{key_name} = :v",
                ExpressionAttributeValues={':v': {'S': value}}
            )
            items.extend(response.get('Items', []))
        return items   
    if invoice_number:
        query_results['invoice'] = query_and_collect(db_table, 'Invoice-Index', 'invoice_number', invoice_number, 'invoice')
    if customer_number:
        query_results['customer'] = query_and_collect(db_table, 'account_number-index', 'account_number', customer_number, 'customer')
    if order_number:
        query_results['order'] = query_and_collect(db_table, 'shipping_number-index', 'shipping_number', order_number, 'order')
    final_results = merge_results(query_results)
 
    deduplicated_dict = {}

    for item in final_results:
        date = int(item.get('date', {}).get('N'))
        if date and (start_date <= date) and (end_date >= date):
            invoice_number = item.get('invoice_number', {}).get('S')
            if invoice_number not in deduplicated_dict:
                deduplicated_dict[invoice_number] = item
    ddb_rows = list(deduplicated_dict.values())

    for ddb_row in ddb_rows:
        bucket_name = ddb_row['BucketName']['S']
        pdf_name_pre = ddb_row['pdf_name']['S']

        if not pdf_name_pre.startswith("I0"):
            pdf_name_pre = "I0"+pdf_name_pre
        if not pdf_name_pre.endswith(".pdf"):
            pdf_name_pre = pdf_name_pre + ".pdf"

        prefix = "invoice/"+pdf_name_pre
        url = generate_presigned_url(s3_bucket_name, prefix)
        ddb_row["bucket_url"] = {"S": url}
    respone = {
        "isBase64Encoded": False,
        "statusCode": 200,
        "statusDescription": "200 OK",
        "headers": {"Set-cookie": "cookies", "Content-Type": "application/json"},
        "body": json.dumps(ddb_rows)
    }
    return respone
    

def parse_range(range_input):
    if '-' in range_input:
        start, end = map(int, range_input.split('-'))
        return list(map(str, range(start, end + 1)))
    if ',' in range_input:
        return [x.strip() for x in range_input.split(',') if x.strip()]
    return [range_input] if range_input else []

def merge_results(results):
    combined_items = []
    for key in results:
        combined_items.extend(results[key])
    return combined_items

def generate_presigned_url(bucket_name, object_key, expiration=3600):
    s3_client = boto3.client('s3')
    url = s3_client.generate_presigned_url(
        ClientMethod='get_object',
        Params={
            'Bucket': bucket_name,
            'Key': object_key
        },
        ExpiresIn=expiration
    )
    return url

def perform_query(table_name, index_name, order_number):
    dynamodb = boto3.client('dynamodb')
    query_params = {
        'TableName': table_name,
        'IndexName': index_name,
        'KeyConditionExpression': 'order_number = :order_number',
        'ExpressionAttributeValues': {
            ':order_number': {'S': order_number}
        }
    }
    response = dynamodb.query(**query_params)
    items = response['Items']
    for item in items:
        return item

def perform_general_query(table_name, index_name, key_name, key_value):
    dynamodb = boto3.client('dynamodb')
    items = []
    query_params = {
        'TableName': table_name,
        'IndexName': index_name,
        'KeyConditionExpression': f'{key_name} = :key_value',
        'ExpressionAttributeValues': {
            ':key_value': {'S': key_value}
        }
    }
    while True:
        response = dynamodb.query(**query_params)
        items.extend(response['Items'])
        last_evaluated_key = response.get('LastEvaluatedKey')
        if last_evaluated_key:
            query_params['ExclusiveStartKey'] = last_evaluated_key
        else:
            break
    return items
