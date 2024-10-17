import os
import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timezone
import random

dynamodb = boto3.resource('dynamodb')
table_name = os.environ['dynamodb_table']
table = dynamodb.Table(table_name)

s3 = boto3.client('s3')

bucket = os.environ['s3_bucket']
logfile = os.environ['s3_logfile']

def lambda_handler(event, context):
    try:
        table_response = table.scan()
    except Exception as e:
        message = f"Error reading table {table_name} to select quote. Please check table and permissions. {e}"
        print(message)
        body = {'message': message}
        status_code = 500
        raise Exception(message)
    
    quotes = table_response['Items']
    random_quote = random.choice(quotes)
    quote_text = random_quote['text']
    author = random_quote['author']    
    
    try:
        log_object = s3.get_object(Bucket=bucket, Key=logfile)
        log_data = log_object['Body'].read().decode('utf-8')
    except ClientError as e:
        print(f"File {logfile} does not exist. Creating empty logfile in bucket {bucket}. {e}")
        if e.response['Error']['Code'] == 'NoSuchKey': s3.put_object(Body="", Bucket=bucket, Key=logfile)
        log_data = ""
    except Exception as e:
        message = f"Error accessing logfile {logfile} in bucket {bucket}. Please check bucket and permissions. {e}"
        print(message)
        status_code = 500
        body = {'message': message}
        raise Exception(message)
    
    try:
        log_message = f"{datetime.now(timezone.utc)} Fetched quote: \"{quote_text}\" - {author}"
        log_data += f"\n{log_message}"
        s3.put_object(Body=log_data, Bucket=bucket, Key=logfile)
    except Exception as e:
        message = f"Error updating logfile {logfile} in bucket {bucket}. Please check bucket and permissions. {e}"
        print(message)
        status_code = 500
        body = {'message': message}
        raise Exception

    print(f"Logged message: {log_message}")
    message = f"Sucessfully fetched random quote and logged it to logfile {logfile} in bucket {bucket}."
    print(message)
    status_code = 200
    body = {
        'message': message,
        'quote': quote_text,
        'author': author
    }
    
    return {
        'statusCode': status_code,
        'body': body
    }