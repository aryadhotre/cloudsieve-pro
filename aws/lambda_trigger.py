# aws/lambda_trigger.py
# AWS Lambda Function — triggered when a file lands in S3 raw bucket
# Deploy this as a Lambda function in AWS Console

import json
import boto3
import urllib.parse
import os

s3_client      = boto3.client('s3')
dynamodb       = boto3.resource('dynamodb')
glue_client    = boto3.client('glue')

TABLE_NAME     = os.environ.get('DYNAMODB_TABLE', 'cloudsieve-jobs')
GLUE_JOB_NAME  = os.environ.get('GLUE_JOB', 'cloudsieve-dedup-job')
CLEAN_BUCKET   = os.environ.get('CLEAN_BUCKET', 'cloudsieve-clean')


def lambda_handler(event, context):
    """Triggered by S3 PutObject event on the raw bucket"""

    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key    = urllib.parse.unquote_plus(record['s3']['object']['key'])
        size   = record['s3']['object']['size']

        print(f"New file detected: s3://{bucket}/{key} ({size} bytes)")

        job_id = key.split('_')[0] if '_' in key else key.replace('.csv','')

        # Save job metadata to DynamoDB
        table = dynamodb.Table(TABLE_NAME)
        table.put_item(Item={
            'job_id':     job_id,
            'filename':   key,
            'bucket':     bucket,
            'size_bytes': size,
            'status':     'UPLOADED',
            'created_at': context.aws_request_id
        })

        # Start Glue ETL job for deduplication
        try:
            response = glue_client.start_job_run(
                JobName=GLUE_JOB_NAME,
                Arguments={
                    '--job_id':      job_id,
                    '--input_path':  f's3://{bucket}/{key}',
                    '--output_path': f's3://{CLEAN_BUCKET}/{job_id}_clean.csv',
                    '--fuzzy_col':   'name',
                    '--threshold':   '85'
                }
            )
            run_id = response['JobRunId']
            print(f"Glue job started: {run_id}")

            # Update DynamoDB with Glue run ID
            table.update_item(
                Key={'job_id': job_id},
                UpdateExpression='SET #s = :s, glue_run_id = :g',
                ExpressionAttributeNames={'#s': 'status'},
                ExpressionAttributeValues={':s': 'PROCESSING', ':g': run_id}
            )

        except Exception as e:
            print(f"Failed to start Glue job: {e}")
            table.update_item(
                Key={'job_id': job_id},
                UpdateExpression='SET #s = :s',
                ExpressionAttributeNames={'#s': 'status'},
                ExpressionAttributeValues={':s': 'FAILED'}
            )
            raise e

    return {'statusCode': 200, 'body': json.dumps('Pipeline triggered successfully')}
