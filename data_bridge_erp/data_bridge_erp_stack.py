from aws_cdk import (
    Duration,
    Stack,
    RemovalPolicy,
    CfnOutput,
    aws_s3 as s3,
    aws_lambda as lambda_,
    aws_apigateway as apigw,
    aws_dynamodb as dynamodb,
)
from constructs import Construct


class DataBridgeErpStack(Stack):
    """
    DataBridge ERP - Serverless Data Ingestion and Transformation Platform
    
    This stack creates:
    - S3 buckets for raw data and Parquet data lake
    - Lambda functions for ingestion, transformation, upload, and status
    - API Gateway REST API with /ingest, /upload, /status endpoints
    - DynamoDB table for job status tracking
    - Uses AWS SDK for pandas Lambda Layer (includes pandas + pyarrow)
    """

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # ============================================================
        # S3 Buckets
        # ============================================================
        
        # Raw data bucket - temporary storage for incoming files
        self.raw_data_bucket = s3.Bucket(
            self, "RawDataBucket",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            lifecycle_rules=[
                s3.LifecycleRule(
                    expiration=Duration.days(7),
                    id="DeleteOldRawFiles"
                )
            ],
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            cors=[
                s3.CorsRule(
                    allowed_methods=[s3.HttpMethods.PUT, s3.HttpMethods.POST],
                    allowed_origins=["*"],
                    allowed_headers=["*"],
                )
            ]
        )

        # Parquet data lake bucket - processed data storage
        self.parquet_bucket = s3.Bucket(
            self, "ParquetDataLake",
            removal_policy=RemovalPolicy.RETAIN,
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            versioned=True,
        )

        # ============================================================
        # DynamoDB Table for Job Status Tracking
        # ============================================================
        
        self.job_status_table = dynamodb.Table(
            self, "JobStatusTable",
            table_name="databridge-job-status",
            partition_key=dynamodb.Attribute(
                name="job_id",
                type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="created_at",
                type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
            time_to_live_attribute="ttl",
        )

        # Add GSI for querying by status
        self.job_status_table.add_global_secondary_index(
            index_name="status-index",
            partition_key=dynamodb.Attribute(
                name="status",
                type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="created_at",
                type=dynamodb.AttributeType.STRING
            ),
        )

        # ============================================================
        # AWS SDK for pandas Layer (includes pandas + pyarrow)
        # https://aws-sdk-pandas.readthedocs.io/en/stable/layers.html
        # ============================================================
        
        # Using the official AWS SDK for pandas layer for ap-south-1
        # Layer ARN format: arn:aws:lambda:<region>:336392948345:layer:AWSSDKPandas-Python312:*
        self.pandas_layer = lambda_.LayerVersion.from_layer_version_arn(
            self, "AWSSDKPandasLayer",
            layer_version_arn=f"arn:aws:lambda:{self.region}:336392948345:layer:AWSSDKPandas-Python312:16"
        )

        # ============================================================
        # Common environment variables for all Lambda functions
        # ============================================================
        
        common_env = {
            "RAW_BUCKET": self.raw_data_bucket.bucket_name,
            "PARQUET_BUCKET": self.parquet_bucket.bucket_name,
            "JOB_TABLE": self.job_status_table.table_name,
        }

        # ============================================================
        # Lambda Functions with AWS SDK for pandas Layer
        # ============================================================
        
        # Ingestion Handler - orchestrates data retrieval
        self.ingestion_lambda = lambda_.Function(
            self, "IngestionHandler",
            function_name="databridge-ingestion-handler",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="ingestion_handler.handler",
            code=lambda_.Code.from_asset("lambda"),
            timeout=Duration.minutes(5),
            memory_size=512,
            environment=common_env,
            layers=[self.pandas_layer],
        )

        # Transform Handler - converts data to Parquet
        self.transform_lambda = lambda_.Function(
            self, "TransformHandler",
            function_name="databridge-transform-handler",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="transform_handler.handler",
            code=lambda_.Code.from_asset("lambda"),
            timeout=Duration.minutes(10),
            memory_size=1024,
            environment=common_env,
            layers=[self.pandas_layer],
        )

        # Upload Handler - handles file uploads from frontend
        self.upload_lambda = lambda_.Function(
            self, "UploadHandler",
            function_name="databridge-upload-handler",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="upload_handler.handler",
            code=lambda_.Code.from_asset("lambda"),
            timeout=Duration.minutes(2),
            memory_size=512,
            environment=common_env,
            layers=[self.pandas_layer],
        )

        # Status Handler - returns job status
        self.status_lambda = lambda_.Function(
            self, "StatusHandler",
            function_name="databridge-status-handler",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="status_handler.handler",
            code=lambda_.Code.from_asset("lambda"),
            timeout=Duration.seconds(30),
            memory_size=256,
            environment=common_env,
        )

        # ============================================================
        # Grant Permissions to Lambda Functions
        # ============================================================
        
        # S3 permissions
        self.raw_data_bucket.grant_read_write(self.ingestion_lambda)
        self.raw_data_bucket.grant_read_write(self.transform_lambda)
        self.raw_data_bucket.grant_read_write(self.upload_lambda)
        
        self.parquet_bucket.grant_read_write(self.transform_lambda)
        
        # DynamoDB permissions
        self.job_status_table.grant_read_write_data(self.ingestion_lambda)
        self.job_status_table.grant_read_write_data(self.transform_lambda)
        self.job_status_table.grant_read_write_data(self.upload_lambda)
        self.job_status_table.grant_read_data(self.status_lambda)
        
        # Lambda invoke permissions
        self.transform_lambda.grant_invoke(self.ingestion_lambda)
        self.transform_lambda.grant_invoke(self.upload_lambda)

        # ============================================================
        # API Gateway
        # ============================================================
        
        self.api = apigw.RestApi(
            self, "DataBridgeApi",
            rest_api_name="DataBridge ERP API",
            description="API for DataBridge ERP data ingestion platform",
            default_cors_preflight_options=apigw.CorsOptions(
                allow_origins=apigw.Cors.ALL_ORIGINS,
                allow_methods=apigw.Cors.ALL_METHODS,
                allow_headers=["Content-Type", "Authorization"],
            ),
            deploy_options=apigw.StageOptions(
                stage_name="v1",
            ),
        )

        # /ingest endpoint - POST
        ingest_resource = self.api.root.add_resource("ingest")
        ingest_resource.add_method(
            "POST",
            apigw.LambdaIntegration(self.ingestion_lambda),
        )

        # /upload endpoint - POST
        upload_resource = self.api.root.add_resource("upload")
        upload_resource.add_method(
            "POST",
            apigw.LambdaIntegration(self.upload_lambda),
        )

        # /status endpoint - GET
        status_resource = self.api.root.add_resource("status")
        status_resource.add_method(
            "GET",
            apigw.LambdaIntegration(self.status_lambda),
        )

        # /status/{job_id} endpoint - GET specific job
        job_status_resource = status_resource.add_resource("{job_id}")
        job_status_resource.add_method(
            "GET",
            apigw.LambdaIntegration(self.status_lambda),
        )

        # ============================================================
        # Outputs
        # ============================================================
        
        CfnOutput(
            self, "ApiEndpoint",
            value=self.api.url,
            description="API Gateway endpoint URL",
        )

        CfnOutput(
            self, "RawBucketName",
            value=self.raw_data_bucket.bucket_name,
            description="Raw data S3 bucket name",
        )

        CfnOutput(
            self, "ParquetBucketName",
            value=self.parquet_bucket.bucket_name,
            description="Parquet data lake S3 bucket name",
        )

        CfnOutput(
            self, "JobStatusTableName",
            value=self.job_status_table.table_name,
            description="DynamoDB job status table name",
        )
