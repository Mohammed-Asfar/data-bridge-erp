"""
DataBridge ERP - Ingestion Handler
Orchestrates data retrieval from various sources (FTP, HTTP, TCP, API)
"""

import json
import os
import uuid
import boto3
from datetime import datetime
from typing import Any

# Initialize AWS clients
s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
lambda_client = boto3.client('lambda')

# Environment variables
RAW_BUCKET = os.environ.get('RAW_BUCKET')
PARQUET_BUCKET = os.environ.get('PARQUET_BUCKET')
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


def create_job_record(job_id: str, source_type: str, source_config: dict) -> dict:
    """Create a new job record in DynamoDB."""
    table = dynamodb.Table(JOB_TABLE)
    
    now = datetime.utcnow().isoformat()
    ttl = int(datetime.utcnow().timestamp()) + (7 * 24 * 60 * 60)  # 7 days TTL
    
    job_record = {
        'job_id': job_id,
        'created_at': now,
        'updated_at': now,
        'status': 'PENDING',
        'source_type': source_type,
        'source_config': json.dumps(source_config),
        'progress': 0,
        'message': 'Job created, waiting for processing',
        'ttl': ttl,
    }
    
    table.put_item(Item=job_record)
    return job_record


def update_job_status(job_id: str, created_at: str, status: str, progress: int, message: str) -> None:
    """Update job status in DynamoDB."""
    table = dynamodb.Table(JOB_TABLE)
    
    table.update_item(
        Key={'job_id': job_id, 'created_at': created_at},
        UpdateExpression='SET #status = :status, progress = :progress, message = :message, updated_at = :updated_at',
        ExpressionAttributeNames={'#status': 'status'},
        ExpressionAttributeValues={
            ':status': status,
            ':progress': progress,
            ':message': message,
            ':updated_at': datetime.utcnow().isoformat(),
        }
    )


def ingest_from_ftp(job_id: str, created_at: str, config: dict) -> dict:
    """Ingest data from FTP server."""
    from connectors.ftp_connector import FTPConnector
    
    update_job_status(job_id, created_at, 'PROCESSING', 10, 'Connecting to FTP server...')
    
    try:
        connector = FTPConnector(
            host=config['host'],
            username=config.get('username'),
            password=config.get('password'),
            port=config.get('port', 21)
        )
        
        update_job_status(job_id, created_at, 'PROCESSING', 30, 'Downloading file from FTP...')
        
        file_content = connector.download_file(config['file_path'])
        filename = os.path.basename(config['file_path'])
        
        # Upload to S3
        s3_key = f"raw/{job_id}/{filename}"
        s3_client.put_object(
            Bucket=RAW_BUCKET,
            Key=s3_key,
            Body=file_content
        )
        
        update_job_status(job_id, created_at, 'PROCESSING', 50, 'File uploaded to S3, triggering transformation...')
        
        return {
            'success': True,
            's3_key': s3_key,
            'filename': filename
        }
        
    except Exception as e:
        update_job_status(job_id, created_at, 'FAILED', 0, f'FTP ingestion failed: {str(e)}')
        raise


def ingest_from_http(job_id: str, created_at: str, config: dict) -> dict:
    """Ingest data from HTTP endpoint."""
    from connectors.http_connector import HTTPConnector
    
    update_job_status(job_id, created_at, 'PROCESSING', 10, 'Fetching data from HTTP endpoint...')
    
    try:
        connector = HTTPConnector(
            url=config['url'],
            headers=config.get('headers', {}),
            auth=config.get('auth')
        )
        
        update_job_status(job_id, created_at, 'PROCESSING', 30, 'Downloading data...')
        
        data = connector.fetch_data(
            method=config.get('method', 'GET'),
            params=config.get('params'),
            body=config.get('body')
        )
        
        # Determine filename from URL or use default
        filename = config.get('filename', f"http_data_{job_id}.json")
        
        # Upload to S3
        s3_key = f"raw/{job_id}/{filename}"
        s3_client.put_object(
            Bucket=RAW_BUCKET,
            Key=s3_key,
            Body=data
        )
        
        update_job_status(job_id, created_at, 'PROCESSING', 50, 'Data uploaded to S3, triggering transformation...')
        
        return {
            'success': True,
            's3_key': s3_key,
            'filename': filename
        }
        
    except Exception as e:
        update_job_status(job_id, created_at, 'FAILED', 0, f'HTTP ingestion failed: {str(e)}')
        raise


def ingest_from_tcp(job_id: str, created_at: str, config: dict) -> dict:
    """Ingest data from TCP endpoint."""
    from connectors.tcp_connector import TCPConnector
    
    update_job_status(job_id, created_at, 'PROCESSING', 10, 'Connecting to TCP endpoint...')
    
    try:
        connector = TCPConnector(
            host=config['host'],
            port=config['port']
        )
        
        update_job_status(job_id, created_at, 'PROCESSING', 30, 'Receiving data...')
        
        data = connector.receive_data(
            send_data=config.get('send_data'),
            timeout=config.get('timeout', 30)
        )
        
        filename = config.get('filename', f"tcp_data_{job_id}.bin")
        
        # Upload to S3
        s3_key = f"raw/{job_id}/{filename}"
        s3_client.put_object(
            Bucket=RAW_BUCKET,
            Key=s3_key,
            Body=data
        )
        
        update_job_status(job_id, created_at, 'PROCESSING', 50, 'Data uploaded to S3, triggering transformation...')
        
        return {
            'success': True,
            's3_key': s3_key,
            'filename': filename
        }
        
    except Exception as e:
        update_job_status(job_id, created_at, 'FAILED', 0, f'TCP ingestion failed: {str(e)}')
        raise


def trigger_transform(job_id: str, created_at: str, s3_key: str, table_name: str) -> None:
    """Trigger the transform Lambda function."""
    lambda_client.invoke(
        FunctionName='databridge-transform-handler',
        InvocationType='Event',  # Async invocation
        Payload=json.dumps({
            'job_id': job_id,
            'created_at': created_at,
            's3_key': s3_key,
            'table_name': table_name
        })
    )


def handler(event: dict, context: Any) -> dict:
    """
    Lambda handler for ingestion requests.
    
    Expected request body:
    {
        "source_type": "ftp|http|tcp|api",
        "table_name": "target_table_name",
        "config": {
            // Source-specific configuration
        }
    }
    """
    try:
        # Parse request body
        body = json.loads(event.get('body', '{}'))
        
        source_type = body.get('source_type')
        table_name = body.get('table_name', 'default')
        config = body.get('config', {})
        
        if not source_type:
            return create_response(400, {
                'error': 'Missing required field: source_type',
                'valid_types': ['ftp', 'http', 'tcp', 'api']
            })
        
        if source_type not in ['ftp', 'http', 'tcp', 'api']:
            return create_response(400, {
                'error': f'Invalid source_type: {source_type}',
                'valid_types': ['ftp', 'http', 'tcp', 'api']
            })
        
        # Generate job ID
        job_id = str(uuid.uuid4())
        
        # Create job record
        job_record = create_job_record(job_id, source_type, config)
        created_at = job_record['created_at']
        
        # Process based on source type
        result = None
        
        if source_type == 'ftp':
            if not config.get('host') or not config.get('file_path'):
                return create_response(400, {
                    'error': 'FTP config requires: host, file_path'
                })
            result = ingest_from_ftp(job_id, created_at, config)
            
        elif source_type == 'http':
            if not config.get('url'):
                return create_response(400, {
                    'error': 'HTTP config requires: url'
                })
            result = ingest_from_http(job_id, created_at, config)
            
        elif source_type == 'tcp':
            if not config.get('host') or not config.get('port'):
                return create_response(400, {
                    'error': 'TCP config requires: host, port'
                })
            result = ingest_from_tcp(job_id, created_at, config)
            
        elif source_type == 'api':
            # API source is similar to HTTP
            if not config.get('url'):
                return create_response(400, {
                    'error': 'API config requires: url'
                })
            result = ingest_from_http(job_id, created_at, config)
        
        if result and result.get('success'):
            # Trigger transformation
            trigger_transform(job_id, created_at, result['s3_key'], table_name)
            
            return create_response(202, {
                'message': 'Ingestion job started',
                'job_id': job_id,
                'status': 'PROCESSING',
                'source_type': source_type,
                'table_name': table_name
            })
        else:
            return create_response(500, {
                'error': 'Ingestion failed',
                'job_id': job_id
            })
            
    except json.JSONDecodeError:
        return create_response(400, {'error': 'Invalid JSON in request body'})
    except Exception as e:
        return create_response(500, {'error': f'Internal server error: {str(e)}'})
