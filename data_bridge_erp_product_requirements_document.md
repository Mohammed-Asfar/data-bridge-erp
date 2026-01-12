# DataBridge ERP

## Product Requirements Document (PRD)

---

## 1. Overview

**Product Name:** DataBridge ERP  
**Purpose:** DataBridge ERP is a secure, serverless data ingestion platform designed to collect ERP data from multiple sources, standardize it, convert it into Parquet format using PyArrow, and store it in Amazon S3. The system provides authentication, job tracking, and monitoring via a web-based frontend.

---

## 2. Goals & Objectives

### Primary Goals
- Ingest ERP data from multiple heterogeneous sources
- Support multiple input file formats
- Convert all ingested data into Parquet format
- Secure the entire system with authentication and authorization
- Track ingestion jobs and statuses

### Business Objectives
- Reduce manual data handling
- Standardize ERP data storage format
- Enable scalable analytics using Parquet
- Ensure auditability and traceability of ingestion jobs

---

## 3. Target Users

| User Type | Description |
|---------|-------------|
| Admin | Manages users, monitors all ingestion jobs |
| Operator | Creates and runs ingestion jobs |
| Viewer | Views ingestion job status and logs |

---

## 4. Functional Requirements

### 4.1 Authentication & Authorization
- Users must log in before accessing the system
- Authentication handled via Amazon Cognito
- Role-based access control (Admin, Operator, Viewer)
- JWT-based authorization for APIs

---

### 4.2 Data Ingestion Sources

The system must support ingesting data from:
- FTP servers
- HTTP endpoints
- TCP sources
- Local file uploads via UI
- API binary uploads

---

### 4.3 Supported Input Formats

| Format | Supported |
|------|----------|
| CSV | Yes |
| JSON | Yes |
| Excel (.xls, .xlsx) | Yes |
| Binary API payload | Yes |

---

### 4.4 Data Transformation
- Detect input file format automatically
- Parse data into Pandas DataFrames
- Convert DataFrame to Parquet using PyArrow
- Validate schema consistency

---

### 4.5 Data Storage

#### Amazon S3
- Store converted Parquet files
- Folder structure:
  ```
  s3://databridge-erp/
    ├── raw/
    └── parquet/
        └── dataset_name/date=YYYY-MM-DD/
  ```

#### DynamoDB
- Store ingestion job metadata
- Track job status and errors

---

### 4.6 Job Tracking

Each ingestion job must store:
- Job ID
- User ID
- Source type
- Input format
- Status (PENDING, RUNNING, SUCCESS, FAILED)
- Output S3 path
- Error message (if any)

---

### 4.7 API Endpoints

| Method | Endpoint | Description |
|------|---------|-------------|
| POST | /ingest | Start ingestion job |
| POST | /upload | Upload local file |
| GET | /jobs | List user jobs |
| GET | /jobs/{job_id} | Get job details |

All endpoints must be protected using Cognito Authorizer.

---

## 5. Non-Functional Requirements

### Performance
- Handle concurrent ingestion jobs
- Convert files up to Lambda size limits

### Scalability
- Serverless architecture
- Auto-scale with workload

### Security
- Cognito-based authentication
- IAM least-privilege access
- No direct S3 access from frontend

### Reliability
- Retry failed jobs
- Log errors to CloudWatch

---

## 6. Frontend Requirements

### Technology
- HTML
- Tailwind CSS
- Vanilla JavaScript (Fetch API)

---

### Pages

#### 6.1 Login Page
- Email & password login
- Cognito authentication

#### 6.2 Dashboard
- Summary of ingestion jobs
- Status indicators

#### 6.3 Create Ingestion Job
- Select source type
- Upload file or enter endpoint
- Submit job

#### 6.4 Job Details Page
- Job metadata
- Status
- Error messages

---

### UI Guidelines
- Responsive design
- Simple, minimal layout
- Clear status colors
  - Green: Success
  - Yellow: Running
  - Red: Failed

---

## 7. Backend Architecture

### Components
- API Gateway
- Lambda (Ingestion & Transform)
- Amazon Cognito
- DynamoDB
- Amazon S3

### Infrastructure
- Provisioned using AWS CDK
- Environment-based stacks (dev, staging, prod)

---

## 8. Error Handling & Logging

- All errors logged to CloudWatch
- Job status updated to FAILED on errors
- User-friendly error messages exposed via API

---

## 9. Future Enhancements

- Schema validation rules
- Glue Data Catalog integration
- Athena query support
- Scheduling ingestion jobs
- Notification system (email/SNS)

---

## 10. Success Metrics

- Successful ingestion rate
- Average processing time per job
- System uptime
- User adoption

---

## 11. Assumptions & Constraints

### Assumptions
- ERP data fits within serverless limits
- Users have valid access credentials

### Constraints
- Lambda execution limits
- File size limits

---

## 12. Appendix

**Project Name:** DataBridge ERP  
**Architecture Type:** Serverless Data Ingestion Platform

