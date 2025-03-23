"""Data generator script for creating and uploading AT&T support records to S3-compatible storage.

This script generates random support records based on allowed values from a MySQL database,
fetches the latest CSV file from an S3 bucket (e.g., MinIO), updates records, and uploads
the new dataset. It supports configurable record counts via command-line arguments and is
designed to run under Data_Generator_Orchestrator.py with a log file specified via the
SCRIPT_LOG_FILE environment variable. All configuration is sourced from environment variables.

Example (run inside python-runner container):
    $ python /app/scripts/CSV_Data_Generator.py 1000
    # Generates 1000 records and uploads them to the configured MinIO bucket.
"""

import argparse
import logging
from typing import List, Tuple, Optional, Union
import pandas as pd
import random
import csv
from datetime import datetime, timedelta
from io import BytesIO, StringIO
import boto3
from botocore.exceptions import ClientError, BotoCoreError
from allowed_values import connect_to_database, fetch_allowed_values, close_database_connection
import os
import sys

# Use log file from environment variable set by orchestrator, with fallback
log_file_name = os.getenv("SCRIPT_LOG_FILE", f"/app/logs/CSV_Data_Generator_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file_name),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def get_s3_client() -> boto3.client:
    """Initialize and return an S3-compatible storage client for MinIO."""
    minio_host = os.getenv("MINIO_HOST")
    minio_port = os.getenv("MINIO_PORT")
    minio_user = os.getenv("MINIO_USER")
    minio_password = os.getenv("MINIO_PASSWORD")

    required_vars = {
        "MINIO_HOST": minio_host,
        "MINIO_PORT": minio_port,
        "MINIO_USER": minio_user,
        "MINIO_PASSWORD": minio_password
    }
    missing_vars = [key for key, value in required_vars.items() if not value]
    if missing_vars:
        logger.error("Missing required MinIO environment variables: %s", ", ".join(missing_vars))
        raise ValueError(f"Missing required MinIO environment variables: {', '.join(missing_vars)}")

    endpoint_url = f"http://{minio_host}:{minio_port}"
    try:
        client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=minio_user,
            aws_secret_access_key=minio_password
        )
        logger.debug("Initialized S3 client for %s", endpoint_url)
        return client
    except Exception as e:
        logger.error("Failed to initialize S3 client: %s", str(e), exc_info=True)
        raise

def get_latest_csv_file(s3_client: boto3.client, bucket_name: str) -> Tuple[Optional[str], Optional[BytesIO]]:
    """Get the latest CSV file from the MinIO bucket."""
    logger.debug("Fetching latest CSV file from bucket: %s", bucket_name)
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix="AT&T_data_")
        if "Contents" not in response or not response["Contents"]:
            logger.warning("No files found with prefix 'AT&T_data_' in bucket: %s", bucket_name)
            return None, None

        latest_object = max(response["Contents"], key=lambda x: x["LastModified"])
        object_key = latest_object["Key"]
        logger.debug("Found latest file: %s", object_key)

        s3_response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        logger.info("Successfully retrieved file: %s", object_key)
        return object_key, BytesIO(s3_response["Body"].read())
    except ClientError as e:
        logger.error("MinIO ClientError: %s", e.response["Error"]["Message"], exc_info=True)
        return None, None
    except BotoCoreError as e:
        logger.error("MinIO BotoCoreError: %s", str(e), exc_info=True)
        return None, None

def get_max_record_id(file_content: Optional[BytesIO], column_name: str) -> int:
    """Get the maximum record ID from the CSV file."""
    if file_content is None:
        logger.debug("No file content provided; returning max record ID as 0")
        return 0
    try:
        file_content.seek(0)
        df = pd.read_csv(file_content, delimiter="|")
        max_id = int(df[column_name].max()) if column_name in df.columns else 0
        logger.debug("Max record ID from file: %d", max_id)
        return max_id
    except Exception as e:
        logger.error("Error reading CSV file for max ID: %s", str(e), exc_info=True)
        return 0

def generate_random_record(
    record_id: int,
    support_categories: List[str],
    agent_pseudo_names: List[str],
    customer_types: List[str]
) -> List[Union[str, None]]:
    """Generate a random record for the dataset with all fields as strings or None."""
    duration = random.randint(20, 600)
    work_time = random.randint(10, duration - 10)
    record = [
        str(record_id),
        random.choice(support_categories),
        random.choice(agent_pseudo_names),
        (datetime.now() - timedelta(minutes=duration)).strftime("%m%d%Y%H%M%S"),
        random.choice(["COMPLETED", "DROPPED", "TRANSFERRED"]),
        random.choice(["CALL", "CHAT", "EMAIL"]),
        random.choice(customer_types),
        str(duration),
        str(work_time),
        random.choice(["RESOLVED", "PENDING RESOLUTION", "WORK IN PROGRESS", "TRANSFERRED TO ANOTHER QUEUE"]),
        str(random.choice([1, 0])),
        random.choice(["SELF-HELP OPTION", "SUPPORT TEAM INTERVENTION"]),
        random.choice(["WORST", "BAD", "NEUTRAL", "GOOD", "BEST"])
    ]
    logger.debug("Generated record with ID: %d", record_id)
    return record

def generate_and_update_records(
    support_categories: List[str],
    agent_pseudo_names: List[str],
    customer_types: List[str],
    start_record_id: int,
    num_records: int
) -> List[List[Union[str, None]]]:
    """Generate random records and simulate updates to existing records."""
    records = []
    record_id = start_record_id
    for _ in range(num_records):
        record_id += 1
        new_record = generate_random_record(record_id, support_categories, agent_pseudo_names, customer_types)
        if random.random() < 0.1:  # 10% chance of a NULL value
            new_record[random.randint(1, 12)] = None
        records.append(new_record)
        if random.random() < 0.25 and record_id > 1:  # 25% chance of updating an existing record
            update_record_id = random.randint(1, record_id)
            updated_record = generate_random_record(update_record_id, support_categories, agent_pseudo_names, customer_types)
            records.append(updated_record)
            logger.debug("Updated record with ID: %d", update_record_id)
    logger.info("Generated %d records (including updates)", len(records))
    return records

def write_records_to_csv(
    s3_client: boto3.client,
    bucket_name: str,
    data: List[List[Union[str, None]]]
) -> str:
    """Write records to CSV and upload to MinIO with all fields quoted."""
    headers = [
        "TICKET_IDENTIFIER", "SUPPORT_CATEGORY", "AGENT_NAME", "DATE_OF_CALL",
        "CALL_STATUS", "CALL_TYPE", "TYPE_OF_CUSTOMER", "DURATION", "WORK_TIME",
        "TICKET_STATUS", "RESOLVED_IN_FIRST_CONTACT", "RESOLUTION_CATEGORY", "RATING"
    ]
    file_name = f"AT&T_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    csv_buffer = StringIO()
    writer = csv.writer(csv_buffer, delimiter="|", quoting=csv.QUOTE_ALL)
    writer.writerow(headers)
    writer.writerows([[str(val) if val is not None else "" for val in row] for row in data])
    
    try:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=file_name,
            Body=csv_buffer.getvalue().encode("utf-8"),
            ContentType="text/csv"
        )
        logger.info("Uploaded file to MinIO: %s/%s", bucket_name, file_name)
        return file_name
    except ClientError as e:
        logger.error("Failed to upload to MinIO: %s", e.response["Error"]["Message"], exc_info=True)
        raise

def main() -> None:
    """Main function to generate and upload random AT&T support records."""
    logger.info("Starting data generation process")

    # Fetch configuration from environment variables
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    minio_bucket = os.getenv("MINIO_BUCKET")

    # Validate environment variables
    required_vars = {
        "DB_HOST": db_host,
        "DB_PORT": db_port,
        "DB_NAME": db_name,
        "DB_USER": db_user,
        "DB_PASSWORD": db_password,
        "MINIO_BUCKET": minio_bucket
    }
    missing_vars = [key for key, value in required_vars.items() if not value]
    if missing_vars:
        logger.error("Missing required environment variables: %s", ", ".join(missing_vars))
        sys.exit(1)

    # Connect to MySQL database
    try:
        engine = connect_to_database(
            db_host,
            db_port,
            db_name,
            db_user,
            db_password,
            logger=logger
        )
    except Exception as e:
        logger.error("Failed to connect to MySQL: %s", str(e), exc_info=True)
        sys.exit(1)

    # Fetch allowed values
    try:
        support_categories = fetch_allowed_values(
            logger, engine, "CSD_SUPPORT_AREAS", "'AT&T'", "SUPPORT_AREA_NAME"
        )
        agent_pseudo_names = fetch_allowed_values(
            logger, engine, "CSD_AGENTS", "'AT&T'", "PSEUDO_CODE"
        )
        customer_types = fetch_allowed_values(
            logger, engine, "CSD_CUSTOMER_TYPES", "'AT&T'", "CUSTOMER_TYPE_NAME"
        )
    except Exception as e:
        logger.error("Failed to fetch allowed values: %s", str(e), exc_info=True)
        close_database_connection(logger, engine)
        sys.exit(1)
    finally:
        close_database_connection(logger, engine)

    # Initialize S3 client and fetch latest file
    try:
        s3_client = get_s3_client()
        file_key, file_content = get_latest_csv_file(s3_client, minio_bucket)
        max_record_id = get_max_record_id(file_content, "TICKET_IDENTIFIER")
    except Exception as e:
        logger.error("Failed to initialize S3 client or fetch latest file: %s", str(e), exc_info=True)
        sys.exit(1)

    # Parse and validate command-line arguments
    parser = argparse.ArgumentParser(description="Generate and upload AT&T support records.")
    parser.add_argument(
        "input",
        nargs="?",
        default=None,
        type=int,
        help="Number of records to generate (default: random between 1 and 10000)"
    )
    args = parser.parse_args()
    num_records = args.input if args.input is not None else random.randint(1, 10000)
    if num_records <= 0:
        logger.error("Number of records must be a positive integer, got %d", num_records)
        sys.exit(1)
    logger.info("Generating %d new records", num_records)

    # Generate and upload records
    try:
        records = generate_and_update_records(
            support_categories, agent_pseudo_names, customer_types, max_record_id, num_records
        )
        file_name = write_records_to_csv(s3_client, minio_bucket, records)
        logger.info("Data generation complete. File uploaded: %s/%s", minio_bucket, file_name)
    except Exception as e:
        logger.error("Failed to generate or upload records: %s", str(e), exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()