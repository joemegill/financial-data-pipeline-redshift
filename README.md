#  Financial Data Pipeline (Redshift + S3)

This repository is the Redshift-enabled variant of the Financial Data Pipeline
project. It ingests portfolio data, uploads raw files to S3, then uses
Redshift's COPY command to load data into a Redshift warehouse. dbt models
sit on top of the Redshift tables to transform data for analytics, and a Streamlit
app serves dashboards.

#
- Uses S3 for staging raw files (boto3)
- Loads data into Redshift using COPY
- dbt configured for Redshift (dbt-redshift)
- Includes LocalStack for local S3 testing (note: Redshift requires an AWS cluster)

## Quickstart (local dev)
1. Copy .env.example to .env and fill in AWS creds (or configure AWS CLI / profile).
2. Create the S3 bucket in your AWS account or use LocalStack (the compose file brings it up).
3. Start local stack and Airflow:
   ```bash
   docker-compose up --build
   ```
4. Create the Redshift tables manually in your Redshift cluster before running the pipeline.
5. Trigger the DAG in Airflow to run the ingestion pipeline.

## Notes
- Redshift is not emulated by LocalStack. For full end-to-end testing you will need a real Redshift cluster (or use Amazon Redshift Serverless).
- Prefer using IAM roles for production Redshift COPY access instead of embedding keys.
