import aws_cdk as core
import aws_cdk.assertions as assertions

from data_bridge_erp.data_bridge_erp_stack import DataBridgeErpStack


def test_s3_buckets_created():
    """Test that S3 buckets are created with correct properties."""
    app = core.App()
    stack = DataBridgeErpStack(app, "data-bridge-erp")
    template = assertions.Template.from_stack(stack)

    # Verify raw data bucket exists
    template.has_resource_properties("AWS::S3::Bucket", {
        "BucketEncryption": {
            "ServerSideEncryptionConfiguration": [
                {
                    "ServerSideEncryptionByDefault": {
                        "SSEAlgorithm": "AES256"
                    }
                }
            ]
        }
    })
    
    # Verify we have 2 S3 buckets
    template.resource_count_is("AWS::S3::Bucket", 2)


def test_dynamodb_table_created():
    """Test that DynamoDB table is created for job status tracking."""
    app = core.App()
    stack = DataBridgeErpStack(app, "data-bridge-erp")
    template = assertions.Template.from_stack(stack)

    template.has_resource_properties("AWS::DynamoDB::Table", {
        "TableName": "databridge-job-status",
        "KeySchema": [
            {"AttributeName": "job_id", "KeyType": "HASH"},
            {"AttributeName": "created_at", "KeyType": "RANGE"}
        ],
        "BillingMode": "PAY_PER_REQUEST"
    })


def test_lambda_functions_created():
    """Test that Lambda functions are created with correct properties."""
    app = core.App()
    stack = DataBridgeErpStack(app, "data-bridge-erp")
    template = assertions.Template.from_stack(stack)

    # Verify Lambda function count (4 functions)
    template.resource_count_is("AWS::Lambda::Function", 4)

    # Check ingestion handler
    template.has_resource_properties("AWS::Lambda::Function", {
        "FunctionName": "databridge-ingestion-handler",
        "Runtime": "python3.12",
        "Handler": "ingestion_handler.handler"
    })

    # Check transform handler
    template.has_resource_properties("AWS::Lambda::Function", {
        "FunctionName": "databridge-transform-handler",
        "Runtime": "python3.12",
        "Handler": "transform_handler.handler"
    })

    # Check upload handler
    template.has_resource_properties("AWS::Lambda::Function", {
        "FunctionName": "databridge-upload-handler",
        "Runtime": "python3.12",
        "Handler": "upload_handler.handler"
    })

    # Check status handler
    template.has_resource_properties("AWS::Lambda::Function", {
        "FunctionName": "databridge-status-handler",
        "Runtime": "python3.12",
        "Handler": "status_handler.handler"
    })


def test_api_gateway_created():
    """Test that API Gateway is created with correct endpoints."""
    app = core.App()
    stack = DataBridgeErpStack(app, "data-bridge-erp")
    template = assertions.Template.from_stack(stack)

    # Verify REST API exists
    template.has_resource_properties("AWS::ApiGateway::RestApi", {
        "Name": "DataBridge ERP API"
    })

    # Verify resources are created
    template.resource_count_is("AWS::ApiGateway::Resource", 4)  # ingest, upload, status, status/{job_id}


def test_iam_role_created():
    """Test that IAM role is created for Lambda execution."""
    app = core.App()
    stack = DataBridgeErpStack(app, "data-bridge-erp")
    template = assertions.Template.from_stack(stack)

    # Verify IAM role exists with Lambda assumed role
    template.has_resource_properties("AWS::IAM::Role", {
        "AssumeRolePolicyDocument": {
            "Statement": [
                {
                    "Action": "sts:AssumeRole",
                    "Effect": "Allow",
                    "Principal": {
                        "Service": "lambda.amazonaws.com"
                    }
                }
            ]
        }
    })


def test_cfn_outputs():
    """Test that CloudFormation outputs are created."""
    app = core.App()
    stack = DataBridgeErpStack(app, "data-bridge-erp")
    template = assertions.Template.from_stack(stack)

    # Verify outputs exist
    template.has_output("ApiEndpoint", {})
    template.has_output("RawBucketName", {})
    template.has_output("ParquetBucketName", {})
    template.has_output("JobStatusTableName", {})
