
import json
import boto3
import os

def lambda_handler(event, context):
    table_name = os.getenv('DYNAMO_DB')
    query_params=event['queryStringParameters']

    invoice_number = query_params.get('invoice_number', '')
    customer_number = query_params.get('customer_number', '')
    is_otb = query_params.get('is_otb', '')
    order_number = query_params.get('pin', '')
    data_complete = query_params.get('dc', '')
    manifest = query_params.get('manifest', '')
    start_date = int(query_params.get('start_date', 0))
    end_date = int(query_params.get('end_date', 99999999))
    date = query_params.get('date','')

    attribute_value_map = {
        'invoice_number': invoice_number,
        'account_number': customer_number
        }

   #Comment below line and added new line from 41 to 43 on 29March 2024
   # ddb_rows = perform_scan2(table_name, attribute_value_map)
   
    ddb_rows = []
    if invoice_number:
        ddb_rows = perform_general_query(table_name, 'Invoice-Index', 'invoice_number', invoice_number)

    transformed_rows = []
    for ddb_row in ddb_rows:
        transformed_rows.append(ddb_row['shipping_number']['S'])
        # bucket_name = ddb_row['BucketName']['S']
        # pdf_name_pre = ddb_row['pdf_name']['S']

        # if not pdf_name_pre.startswith("I0"):
        #     pdf_name_pre = "I0"+pdf_name_pre
        # if not pdf_name_pre.endswith(".pdf"):
        #     pdf_name_pre = pdf_name_pre + ".pdf"

        # prefix = "invoice/"+pdf_name_pre
        # url = generate_presigned_url(bucket_name, prefix)
        # ddb_row["bucket_url"] = {"S": url}

    row_set = list(set(transformed_rows))




    respone = {
        "isBase64Encoded": False,
        "statusCode": 200,
        "statusDescription": "200 OK",
        "headers": {"Set-cookie": "cookies", "Content-Type": "application/json"},
        "body": json.dumps(row_set), #### This is important. It expects the event to be in json. so, it will throw 502, if you just put event.
    }

    return respone

def perform_scan2(table_name, attribute_value_map, exclusive_start_key=None):
    dynamodb = boto3.client('dynamodb')
    expression_attribute_names = {}
    expression_attribute_values = {}
    filter_expression_parts = []

    for attribute_name, attribute_value in attribute_value_map.items():
        if attribute_value:
            if '-' in attribute_value:
                start, end = attribute_value.split('-')
                filter_expression_parts.append(f'#{attribute_name} BETWEEN :start AND :end')
                expression_attribute_values[':start'] = {'S': start}
                expression_attribute_values[':end'] = {'S': end}
            else:
                filter_expression_parts.append(f'#{attribute_name} = :{attribute_name}')
                expression_attribute_values[f':{attribute_name}'] = {'S': attribute_value}
            expression_attribute_names[f'#{attribute_name}'] = attribute_name

    filter_expression = ' AND '.join(filter_expression_parts)

    items = []

    while True:
        scan_params = {
            'TableName': table_name,
            'FilterExpression': filter_expression,
            'ExpressionAttributeNames': expression_attribute_names,
            'ExpressionAttributeValues': expression_attribute_values,
        }

        if exclusive_start_key:
            scan_params['ExclusiveStartKey'] = exclusive_start_key

        response = dynamodb.scan(**scan_params)

        items.extend(response['Items'])
        exclusive_start_key = response.get('LastEvaluatedKey')

        if not exclusive_start_key:
            break

    return items

def generate_presigned_url(bucket_name, object_key, expiration=3600):
    # Create an S3 client
    s3_client = boto3.client('s3')

    # Generate a presigned URL for the S3 object
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
    # Create a DynamoDB client
    dynamodb = boto3.client('dynamodb')

    # Define the query parameters
    query_params = {
        'TableName': table_name,
        'IndexName': index_name,
        'KeyConditionExpression': 'order_number = :order_number',
        'ExpressionAttributeValues': {
            ':order_number': {'S': order_number}
        }
    }

    response = dynamodb.query(**query_params)
    # Process the query results
    items = response['Items']
    for item in items:
        return item
        
#Added new function on 29March2024

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
 
        # Check if there are more results available for pagination
        last_evaluated_key = response.get('LastEvaluatedKey')
        if last_evaluated_key:
            query_params['ExclusiveStartKey'] = last_evaluated_key
        else:
            break
 
    return items