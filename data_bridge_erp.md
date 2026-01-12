# Product Requirements Document (PRD)

## Project Name
**DataBridge ERP**

---

## 1. Overview

**DataBridge ERP** is a serverless data ingestion and transformation platform designed to ingest ERP data from multiple external and internal sources, convert the data into **Parquet format using PyArrow**, and store it securely in Amazon S3 for analytics and downstream processing.

The system is built using **AWS CDK** for infrastructure as code and provides a **web-based frontend** for triggering ingestion jobs, uploading files, and monitoring processing status.

---

## 2. Goals & Objectives

### Primary Goals
- Ingest ERP data from multiple source types
- Standardize all incoming data into **Parquet format**
- Store processed data in Amazon S3
- Provide a simple frontend to control ingestion
- Ensure scalability, reliability, and low operational cost

### Success Metrics
- Successful ingestion and conversion of supported file types
- Parquet files generated correctly using PyArrow
- System scales automatically with workload
- Minimal manual intervention required
- Clear visibility into ingestion job status

---

## 3. Scope

### In Scope
- Serverless ingestion pipeline
- File transformation to Parquet
- AWS CDK-based infrastructure
- Frontend UI for ingestion control
- Logging and monitoring

### Out of Scope (Phase 1)
- Real-time streaming ingestion
- Data cleansing or enrichment
- Complex schema evolution handling
- BI dashboards

---

## 4. Functional Requirements

### 4.1 Input Data Sources
The system must support ingestion from the following sources:
- FTP servers
- TCP endpoints
- HTTP endpoints
- API-based binary uploads
- Local file uploads via frontend UI

---

### 4.2 Supported Input Formats
The system must accept the following file formats:
- CSV
- JSON
- Excel (`.xls`, `.xlsx`)
- Binary payloads (API uploads)

---

### 4.3 Data Processing
- Raw files must be temporarily stored during processing
- Data must be loaded into Pandas DataFrames
- Conversion to Parquet must be done **strictly using PyArrow**
- No alternative Parquet libraries are allowed

---

### 4.4 Output Data
- Output format: **Parquet**
- Storage location: **Amazon S3**
- Data must be partitioned by:
  - Table name
  - Processing date (YYYY-MM-DD)

---

### 4.5 API Requirements

| Method | Endpoint | Description |
|-----|---------|------------|
| POST | `/ingest` | Trigger ingestion from FTP/HTTP/API |
| POST | `/upload` | Upload file from frontend |
| GET | `/status` | Fetch ingestion job status |

---

### 4.6 Frontend Requirements
The frontend must provide:
- Source type selection
- Credential input for FTP/API
- File upload capability
- Ingestion trigger button
- Job status display
- Basic logs or error messages

---

## 5. Non-Functional Requirements

### 5.1 Performance
- Lambda execution must complete within AWS limits
- Parallel ingestion jobs must be supported

### 5.2 Scalability
- System must scale automatically based on load
- No fixed infrastructure capacity

### 5.3 Security
- IAM-based access control
- Secure handling of credentials
- S3 bucket access restricted by role
- HTTPS for all API endpoints

### 5.4 Reliability
- Failed jobs must be logged
- Errors must be visible in CloudWatch
- Partial failures should not corrupt data

---

## 6. Architecture Overview

### High-Level Flow

Frontend UI
↓
API Gateway
↓
Ingestion Lambda
↓
Source Connectors (FTP / HTTP / API / Upload)
↓
Transform Lambda
↓
PyArrow Conversion
↓
Amazon S3 (Parquet Data Lake)

## 7. AWS Services Used

- Amazon S3 – Raw and Parquet storage
- AWS Lambda (Python) – Ingestion and transformation
- Amazon API Gateway – API endpoints
- AWS CDK – Infrastructure as Code
- Amazon CloudWatch – Logging and monitoring
- IAM – Security and permissions
