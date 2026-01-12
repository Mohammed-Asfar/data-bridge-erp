"""
DataBridge ERP - Upload Handler
Handles file uploads from the frontend UI
"""

import json
import os
import uuid
import base64
import boto3
from datetime import datetime
from typing import Any

# Initialize AWS clients
s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
lambda_client = boto3.client('lambda')

# Environment variables
RAW_BUCKET = os.environ.get('RAW_BUCKET')
JOB_TABLE = os.environ.get('JOB_TABLE')


def create_response(status_code: int, body: dict) -> dict:
    """Create API Gateway response with CORS headers."""
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': 'Content-Type,Authorization',
            'Access-Control-Allow-Methods': 'POST,OPTIONS',
        },
        'body': json.dumps(body)
    }


def create_job_record(job_id: str, filename: str, table_name: str) -> dict:
    """Create a new job record in DynamoDB."""
    table = dynamodb.Table(JOB_TABLE)
    
    now = datetime.utcnow().isoformat()
    ttl = int(datetime.utcnow().timestamp()) + (7 * 24 * 60 * 60)  # 7 days TTL
    
    job_record = {
        'job_id': job_id,
        'created_at': now,
        'updated_at': now,
        'status': 'PENDING',
        'source_type': 'upload',
        'filename': filename,
        'table_name': table_name,
        'progress': 0,
        'message': 'File uploaded, waiting for processing',
        'ttl': ttl,
    }
    
    table.put_item(Item=job_record)
    return job_record


def validate_file_format(filename: str) -> tuple[bool, str]:
    """Validate that the file format is supported."""
    supported_extensions = ['csv', 'json', 'xls', 'xlsx', 'txt']
    
    if '.' not in filename:
        return False, 'File must have an extension'
    
    ext = filename.lower().split('.')[-1]
    
    if ext not in supported_extensions:
        return False, f'Unsupported file format: .{ext}. Supported: {", ".join(supported_extensions)}'
    
    return True, 'OK'


def handler(event: dict, context: Any) -> dict:
    """
    Lambda handler for file uploads.
    
    Expected request body:
    {
        "filename": "data.csv",
        "table_name": "target_table_name",  // optional
        "content": "base64_encoded_file_content"
    }
    
    Or with isBase64Encoded flag from API Gateway binary support.
    """
    try:
        # Handle API Gateway base64 encoding
        is_base64 = event.get('isBase64Encoded', False)
        
        if is_base64:
            # Binary upload via API Gateway
            body_content = base64.b64decode(event.get('body', ''))
            # Get filename from headers or query params
            headers = event.get('headers', {})
            query_params = event.get('queryStringParameters', {}) or {}
            
            filename = (
                headers.get('x-filename') or
                headers.get('X-Filename') or
                query_params.get('filename') or
                f'upload_{datetime.utcnow().strftime("%Y%m%d_%H%M%S")}.bin'
            )
            table_name = query_params.get('table_name', 'default')
            file_content = body_content
        else:
            # JSON body with base64 content
            body = json.loads(event.get('body', '{}'))
            
            filename = body.get('filename')
            table_name = body.get('table_name', 'default')
            content_b64 = body.get('content')
            
            if not filename:
                return create_response(400, {'error': 'Missing required field: filename'})
            
            if not content_b64:
                return create_response(400, {'error': 'Missing required field: content'})
            
            try:
                file_content = base64.b64decode(content_b64)
            except Exception:
                return create_response(400, {'error': 'Invalid base64 content'})
        
        # Validate file format
        is_valid, message = validate_file_format(filename)
        if not is_valid:
            return create_response(400, {'error': message})
        
        # Validate file size (max 10MB for Lambda)
        max_size = 10 * 1024 * 1024  # 10MB
        if len(file_content) > max_size:
            return create_response(400, {
                'error': f'File too large. Maximum size: {max_size // (1024*1024)}MB'
            })
        
        # Generate job ID
        job_id = str(uuid.uuid4())
        
        # Upload to S3
        s3_key = f"raw/{job_id}/{filename}"
        
        s3_client.put_object(
            Bucket=RAW_BUCKET,
            Key=s3_key,
            Body=file_content,
            Metadata={
                'job_id': job_id,
                'original_filename': filename,
                'table_name': table_name,
            }
        )
        
        # Create job record
        job_record = create_job_record(job_id, filename, table_name)
        
        # Trigger transformation
        lambda_client.invoke(
            FunctionName='databridge-transform-handler',
            InvocationType='Event',  # Async
            Payload=json.dumps({
                'job_id': job_id,
                'created_at': job_record['created_at'],
                's3_key': s3_key,
                'table_name': table_name
            })
        )
        
        return create_response(202, {
            'message': 'File uploaded successfully',
            'job_id': job_id,
            'filename': filename,
            'table_name': table_name,
            'status': 'PROCESSING',
            's3_key': s3_key
        })
        
    except json.JSONDecodeError:
        return create_response(400, {'error': 'Invalid JSON in request body'})
    except Exception as e:
        print(f"Error: {str(e)}")
        return create_response(500, {'error': f'Internal server error: {str(e)}'})
