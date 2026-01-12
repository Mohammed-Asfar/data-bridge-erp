"""
DataBridge ERP - Status Handler
Returns job status information from DynamoDB
"""

import json
import os
import boto3
from decimal import Decimal
from typing import Any
from boto3.dynamodb.conditions import Key

# Initialize AWS clients
dynamodb = boto3.resource('dynamodb')

# Environment variables
JOB_TABLE = os.environ.get('JOB_TABLE')


class DecimalEncoder(json.JSONEncoder):
    """Custom JSON encoder that handles Decimal types from DynamoDB."""
    def default(self, obj):
        if isinstance(obj, Decimal):
            # Convert to int if it's a whole number, otherwise float
            if obj % 1 == 0:
                return int(obj)
            return float(obj)
        return super().default(obj)


def create_response(status_code: int, body: dict) -> dict:
    """Create API Gateway response with CORS headers."""
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': 'Content-Type,Authorization',
            'Access-Control-Allow-Methods': 'GET,OPTIONS',
        },
        'body': json.dumps(body, cls=DecimalEncoder)
    }


def get_job_by_id(job_id: str) -> dict:
    """Get job details by job ID."""
    table = dynamodb.Table(JOB_TABLE)
    
    # Query by partition key
    response = table.query(
        KeyConditionExpression=Key('job_id').eq(job_id),
        ScanIndexForward=False,  # Most recent first
        Limit=1
    )
    
    items = response.get('Items', [])
    
    if not items:
        return None
    
    return items[0]


def list_jobs(status: str = None, limit: int = 20) -> list:
    """List jobs, optionally filtered by status."""
    table = dynamodb.Table(JOB_TABLE)
    
    if status:
        # Query by status using GSI
        response = table.query(
            IndexName='status-index',
            KeyConditionExpression=Key('status').eq(status),
            ScanIndexForward=False,
            Limit=limit
        )
    else:
        # Scan all jobs (limited)
        response = table.scan(Limit=limit)
    
    items = response.get('Items', [])
    
    # Sort by created_at descending
    items.sort(key=lambda x: x.get('created_at', ''), reverse=True)
    
    return items[:limit]


def format_job_response(job: dict) -> dict:
    """Format job record for API response."""
    return {
        'job_id': job.get('job_id'),
        'status': job.get('status'),
        'progress': job.get('progress', 0),
        'message': job.get('message'),
        'source_type': job.get('source_type'),
        'table_name': job.get('table_name'),
        'filename': job.get('filename'),
        'created_at': job.get('created_at'),
        'updated_at': job.get('updated_at'),
        'output_key': job.get('output_key'),
        'row_count': job.get('row_count'),
        'column_count': job.get('column_count'),
    }


def handler(event: dict, context: Any) -> dict:
    """
    Lambda handler for status queries.
    
    Endpoints:
    - GET /status - List all jobs
    - GET /status?status=COMPLETED - List jobs by status
    - GET /status/{job_id} - Get specific job
    """
    try:
        # Get path parameters
        path_params = event.get('pathParameters') or {}
        job_id = path_params.get('job_id')
        
        # Get query parameters
        query_params = event.get('queryStringParameters') or {}
        status_filter = query_params.get('status')
        limit = int(query_params.get('limit', 20))
        
        # Limit max results
        limit = min(limit, 100)
        
        if job_id:
            # Get specific job
            job = get_job_by_id(job_id)
            
            if not job:
                return create_response(404, {
                    'error': 'Job not found',
                    'job_id': job_id
                })
            
            return create_response(200, {
                'job': format_job_response(job)
            })
        else:
            # List jobs
            jobs = list_jobs(status=status_filter, limit=limit)
            
            return create_response(200, {
                'jobs': [format_job_response(job) for job in jobs],
                'count': len(jobs),
                'filter': {
                    'status': status_filter,
                    'limit': limit
                }
            })
            
    except Exception as e:
        print(f"Error: {str(e)}")
        return create_response(500, {'error': f'Internal server error: {str(e)}'})
