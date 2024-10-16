import json
import os
import boto3
import uuid
import datetime

dynamodb_client = boto3.client('dynamodb')
s3_client = boto3.client('s3')
table = os.environ['dynamo_db_table']
bucket = os.environ['s3_bucket']
logfile_key = os.environ['s3_logfile']

def lambda_handler(event, context):
    rand_uuid = str(uuid.uuid4())
    scan = True
    
    while scan:
        random_quote = dynamodb_client.scan(
            TableName=table,
            ExclusiveStartKey={
                'quote_id': {
                    'S': rand_uuid
                }
            },
            Limit=1
        )
        if len(random_quote['Items']) > 0:
            scan = False
        
    quote_text = random_quote['Items'][0]['text']['S']
    author = random_quote['Items'][0]['author']['S']
    
    
    log_message = f"{datetime.datetime.utcnow()} Fetched quote: \"{quote_text}\" - {author}"
    
    try:
        quote_log_object = s3_client.get_object(Bucket=bucket, Key=logfile_key)
        log_data = quote_log_object['Body'].read().decode('utf-8')
        log_data += f"\n{log_message}"
    except Exception as e:
        print(e)
        log_data = log_message
    finally:
        print(log_data)
        s3_client.put_object(Body=log_data, Bucket=bucket, Key=logfile_key)
    
    
    payload = {
        'statusCode': 200,
        'body': {
            'quote': quote_text,
            'author': author
        }
    }
    return payload
