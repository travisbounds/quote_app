import boto3
import csv
import os

def lambda_handler(event, context):
    dynamodb = boto3.resource('dynamodb')
    table_name = 'quotes-cftest1'
    table = dynamodb.Table(table_name)
    
    s3 = boto3.client('s3')
    bucket = 'quotes-cftest1'
    file_key = 'quotes.csv'
    
    s3_object = s3.get_object(Bucket=bucket, Key=file_key)
    file_content = s3_object['Body'].read().decode('utf-8').splitlines()
    file_dict = csv.DictReader(file_content)
    
    item_count = 0
    error_count = 0
    errors = []

    with table.batch_writer() as batch:
        for item in file_dict:
            try:
                print(f"trying {item}")
                batch.put_item(Item={
                        'quote_id': item['quote_id'],
                        'text': item['quote_text'],
                        'author': item['author'],
                        'date': item['Date']
                    }
                )
                item_count += 1
            except Exception as e:
                print(f"Error inserting item {item['quote_id']}: {e}")
                errors.append(item)
                error_count += 1

    return {
        'statusCode': 200,
        'body': f'Import of {file_key} completed. {item_count} rows inserted into {table_name} table with {error_count} errors.',
        'errors': errors
    }
