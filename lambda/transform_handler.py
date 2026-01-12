"""
DataBridge ERP - Transform Handler
Converts raw data files to Parquet format using PyArrow
"""

import json
import os
import io
import boto3
from datetime import datetime
from typing import Any, Optional

# Initialize AWS clients
s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

# Environment variables
RAW_BUCKET = os.environ.get('RAW_BUCKET')
PARQUET_BUCKET = os.environ.get('PARQUET_BUCKET')
JOB_TABLE = os.environ.get('JOB_TABLE')


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


def detect_file_format(filename: str) -> str:
    """Detect file format from filename extension."""
    ext = filename.lower().split('.')[-1]
    
    format_map = {
        'csv': 'csv',
        'json': 'json',
        'xls': 'excel',
        'xlsx': 'excel',
        'parquet': 'parquet',
        'txt': 'csv',  # Assume CSV for text files
    }
    
    return format_map.get(ext, 'binary')


def load_dataframe(file_content: bytes, file_format: str, filename: str):
    """Load file content into a Pandas DataFrame."""
    import pandas as pd
    
    if file_format == 'csv':
        # Try to detect encoding and delimiter
        try:
            df = pd.read_csv(io.BytesIO(file_content))
        except Exception:
            # Try with different encoding
            df = pd.read_csv(io.BytesIO(file_content), encoding='latin-1')
    
    elif file_format == 'json':
        content_str = file_content.decode('utf-8')
        # Try to parse as JSON lines or regular JSON
        try:
            df = pd.read_json(io.StringIO(content_str), lines=True)
        except ValueError:
            df = pd.read_json(io.StringIO(content_str))
    
    elif file_format == 'excel':
        import openpyxl  # Ensure openpyxl is available
        df = pd.read_excel(io.BytesIO(file_content), engine='openpyxl')
    
    elif file_format == 'parquet':
        # Already in Parquet format
        import pyarrow.parquet as pq
        table = pq.read_table(io.BytesIO(file_content))
        df = table.to_pandas()
    
    else:
        raise ValueError(f'Unsupported file format: {file_format}')
    
    return df


def convert_to_parquet(df, output_path: str) -> bytes:
    """
    Convert Pandas DataFrame to Parquet format using PyArrow.
    
    IMPORTANT: Per PRD requirements, conversion MUST use PyArrow.
    No alternative Parquet libraries are allowed.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq
    
    # Convert DataFrame to PyArrow Table
    table = pa.Table.from_pandas(df)
    
    # Write to Parquet in memory
    buffer = io.BytesIO()
    pq.write_table(
        table,
        buffer,
        compression='snappy',  # Good balance of speed and compression
        use_dictionary=True,
        write_statistics=True,
    )
    
    buffer.seek(0)
    return buffer.read()


def handler(event: dict, context: Any) -> dict:
    """
    Lambda handler for data transformation.
    
    Expected event (from ingestion handler):
    {
        "job_id": "uuid",
        "created_at": "iso-timestamp",
        "s3_key": "raw/job_id/filename",
        "table_name": "target_table_name"
    }
    """
    job_id = event.get('job_id')
    created_at = event.get('created_at')
    s3_key = event.get('s3_key')
    table_name = event.get('table_name', 'default')
    
    if not all([job_id, created_at, s3_key]):
        print(f"Missing required fields: job_id={job_id}, created_at={created_at}, s3_key={s3_key}")
        return {'statusCode': 400, 'body': 'Missing required fields'}
    
    try:
        update_job_status(job_id, created_at, 'TRANSFORMING', 55, 'Starting data transformation...')
        
        # Download raw file from S3
        update_job_status(job_id, created_at, 'TRANSFORMING', 60, 'Downloading raw file from S3...')
        
        response = s3_client.get_object(Bucket=RAW_BUCKET, Key=s3_key)
        file_content = response['Body'].read()
        filename = s3_key.split('/')[-1]
        
        # Detect file format
        file_format = detect_file_format(filename)
        update_job_status(job_id, created_at, 'TRANSFORMING', 65, f'Detected format: {file_format}')
        
        if file_format == 'binary':
            update_job_status(job_id, created_at, 'FAILED', 0, f'Unsupported file format for: {filename}')
            return {'statusCode': 400, 'body': 'Unsupported file format'}
        
        # Load into DataFrame
        update_job_status(job_id, created_at, 'TRANSFORMING', 70, 'Loading data into DataFrame...')
        df = load_dataframe(file_content, file_format, filename)
        
        row_count = len(df)
        col_count = len(df.columns)
        update_job_status(
            job_id, created_at, 'TRANSFORMING', 80,
            f'Loaded {row_count} rows, {col_count} columns. Converting to Parquet...'
        )
        
        # Convert to Parquet using PyArrow
        parquet_data = convert_to_parquet(df, None)
        
        # Generate partitioned S3 key: table_name/YYYY-MM-DD/filename.parquet
        processing_date = datetime.utcnow().strftime('%Y-%m-%d')
        base_filename = os.path.splitext(filename)[0]
        parquet_key = f"{table_name}/{processing_date}/{base_filename}_{job_id}.parquet"
        
        update_job_status(job_id, created_at, 'TRANSFORMING', 90, 'Uploading Parquet file to data lake...')
        
        # Upload Parquet to data lake
        s3_client.put_object(
            Bucket=PARQUET_BUCKET,
            Key=parquet_key,
            Body=parquet_data,
            ContentType='application/octet-stream',
            Metadata={
                'job_id': job_id,
                'source_file': filename,
                'row_count': str(row_count),
                'column_count': str(col_count),
                'processing_date': processing_date,
            }
        )
        
        # Update job status to completed
        update_job_status(
            job_id, created_at, 'COMPLETED', 100,
            f'Successfully converted to Parquet. Output: s3://{PARQUET_BUCKET}/{parquet_key}'
        )
        
        # Optionally, update job record with output location
        table = dynamodb.Table(JOB_TABLE)
        table.update_item(
            Key={'job_id': job_id, 'created_at': created_at},
            UpdateExpression='SET output_key = :output_key, row_count = :rows, column_count = :cols',
            ExpressionAttributeValues={
                ':output_key': parquet_key,
                ':rows': row_count,
                ':cols': col_count,
            }
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Transformation completed',
                'job_id': job_id,
                'output_key': parquet_key,
                'row_count': row_count,
                'column_count': col_count
            })
        }
        
    except Exception as e:
        error_message = f'Transformation failed: {str(e)}'
        print(f"Error: {error_message}")
        
        if job_id and created_at:
            update_job_status(job_id, created_at, 'FAILED', 0, error_message)
        
        return {
            'statusCode': 500,
            'body': json.dumps({'error': error_message})
        }
