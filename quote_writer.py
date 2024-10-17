import boto3
import csv
import os

def lambda_handler(event, context):
    dynamodb = boto3.resource('dynamodb')
    table_name = os.environ['dynamodb_table']
    table = dynamodb.Table(table_name)
    
    s3 = boto3.client('s3')
    bucket = os.environ['source_bucket']
    quote_source_filename = os.environ['quotes_source_file']
    
    try:
        quote_data_object = s3.get_object(Bucket=bucket, Key=quote_source_filename)
    except Exception as e:
        message = f"Error fetching file {quote_source_filename} from source bucket {bucket}. Please check source bucket and file. {e}"
        status_code = 500
        print(message)
        raise Exception(message)
    try:
        decoded_object = quote_data_object['Body'].read().decode('utf-8')
        quotes = csv.DictReader(decoded_object.splitlines())
    except Exception as e:
        message = f"Error decoding file {quote_source_filename}. Please check source file. {e}"
        status_code = 500
        print(message)
        raise Exception(message)
    item_count = 0
    error_count = 0
    errored_items = []

    try:
        with table.batch_writer() as batch:
            for item in quotes:
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
                    print(f"Error inserting quote_id:{item['quote_id']} {e}")
                    errored_items.append(item)
                    error_count += 1
            message = f'Import of {quote_source_filename} completed. {item_count} rows inserted into {table_name} table with {error_count} errors.'
            status_code = 200
    except Exception as e:
        message = f"Error populating quote data in table {table_name}. Please check table and permissions. {e}"
        status_code = 500
        print(message)
        raise Exception(message)
    return {
        'statusCode': status_code,
        'body': message,
        'errors': errored_items
    }
