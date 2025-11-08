import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def load_to_redshift(s3_key, table_name, iam_role_arn=None):
    """Use Redshift COPY to load Parque from S3 into Redshift table."""
    redshift_host = os.getenv('REDSHIFT_HOST')
    redshift_port = os.getenv('REDSHIFT_PORT', '5439')
    redshift_db = os.getenv('REDSHIFT_DB')
    redshift_user = os.getenv('REDSHIFT_USER')
    redshift_password = os.getenv('REDSHIFT_PASSWORD')
    s3_bucket = os.getenv('S3_BUCKET_NAME')
    aws_key = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret = os.getenv('AWS_SECRET_ACCESS_KEY')

    conn = psycopg2.connect(
        dbname=redshift_db,
        user=redshift_user,
        password=redshift_password,
        host=redshift_host,
        port=redshift_port
    )
    cur = conn.cursor()

    # If you have an IAM role for Redshift to read S3, prefer that.
    if iam_role_arn:
        creds = f"iam_role '{iam_role_arn}'"
    else:
        creds = f"ACCESS_KEY_ID '{aws_key}' SECRET_ACCESS_KEY '{aws_secret}'"

    copy_cmd = f"""
    COPY {table_name}
    FROM 's3://{s3_bucket}/{s3_key}'
    {creds}
    CSV
    IGNOREHEADER 1
    REGION AS '{os.getenv('AWS_DEFAULT_REGION', 'us-east-2')}';
    """

    print('Executing COPY command...')
    cur.execute(copy_cmd)
    conn.commit()
    cur.close()
    conn.close()
    print(f"Loaded data into Redshift table {table_name}.")
