"""Data generator script for creating and sending Amazon support records to Kafka.

This script generates random support records in XML format based on allowed values from a MySQL database,
sends them to a Kafka topic, and tracks the highest serial number and record ID in an S3-compatible storage
(e.g., MinIO). It supports configurable record counts via command-line arguments and is designed to run
under Data_Generator_Orchestrator.py with a log file specified via the SCRIPT_LOG_FILE environment variable.
All configuration is sourced from environment variables.

Example (run inside python-runner container):
    $ python /app/scripts/XML_Data_Generator.py 200
    # Generates 200 records and sends them to the configured Kafka topic.
"""

import random
import logging
from typing import List, Tuple
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
import argparse
from confluent_kafka import Producer, KafkaError
import boto3
from botocore.exceptions import ClientError
import json
from allowed_values import connect_to_database, fetch_allowed_values, close_database_connection
import os
import sys

# Use log file from environment variable set by orchestrator, with fallback
log_file_name = os.getenv("SCRIPT_LOG_FILE", f"/app/logs/XML_Data_Generator_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
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

def get_initial_values_from_s3(
    s3_client: boto3.client,
    bucket_name: str,
    file_name: str
) -> Tuple[int, int]:
    """Read the highest serial_number and record_id from a MinIO file."""
    logger.debug("Fetching initial values from MinIO: %s/%s", bucket_name, file_name)
    try:
        try:
            s3_client.head_bucket(Bucket=bucket_name)
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                logger.info("Bucket %s not found; creating it", bucket_name)
                s3_client.create_bucket(Bucket=bucket_name)
            else:
                raise

        response = s3_client.get_object(Bucket=bucket_name, Key=file_name)
        data = json.loads(response['Body'].read().decode('utf-8'))
        max_serial_number = data.get("max_serial_number", 0)
        max_record_id = data.get("max_record_id", 0)
        logger.debug("Retrieved max_serial_number: %d, max_record_id: %d", max_serial_number, max_record_id)
        return max_serial_number, max_record_id
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            logger.info("No file '%s' found in bucket '%s'. Starting from 0", file_name, bucket_name)
            return 0, 0
        logger.error("MinIO error fetching %s: %s", file_name, str(e), exc_info=True)
        return 0, 0

def write_highest_values_to_s3(
    s3_client: boto3.client,
    bucket_name: str,
    file_name: str,
    max_serial_number: int,
    max_record_id: int
) -> None:
    """Write the highest serial_number and record_id to a file in MinIO."""
    data = {"max_serial_number": max_serial_number, "max_record_id": max_record_id}
    json_data = json.dumps(data).encode('utf-8')
    try:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=file_name,
            Body=json_data,
            ContentType="application/json"
        )
        logger.debug("Wrote max_serial_number: %d, max_record_id: %d to %s/%s", 
                     max_serial_number, max_record_id, bucket_name, file_name)
    except ClientError as e:
        logger.error("Failed to write to MinIO %s/%s: %s", bucket_name, file_name, str(e), exc_info=True)
        raise

def produce_record_to_kafka(
    producer: Producer,
    topic: str,
    xml_data: ET.Element
) -> None:
    """Send the XML data to a Kafka topic."""
    xml_string = ET.tostring(xml_data, encoding="unicode")
    try:
        producer.produce(topic, xml_string.encode('utf-8'))
        producer.poll(0)
        logger.debug("Produced record to Kafka topic %s: %s", topic, xml_string[:50] + "...")
    except KafkaError as e:
        logger.error("Failed to produce to Kafka topic %s: %s", topic, str(e), exc_info=True)
        raise

def generate_random_record(
    serial_number: int,
    record_id: int,
    support_categories: List[str],
    agent_pseudo_names: List[str],
    customer_types: List[str]
) -> ET.Element:
    """Generate a random record with specified data fields in XML format."""
    contact_duration_seconds = random.randint(20, 600)
    after_contact_work_time_seconds = random.randint(10, contact_duration_seconds - 10)

    record = ET.Element("RECORD")
    ET.SubElement(record, "SERIAL_NUMBER").text = str(serial_number)
    ET.SubElement(record, "SUPPORT_IDENTIFIER").text = str(record_id)
    ET.SubElement(record, "CONTACT_REGARDING").text = random.choice(support_categories)
    ET.SubElement(record, "AGENT_CODE").text = random.choice(agent_pseudo_names)
    ET.SubElement(record, "DATE_OF_INTERACTION").text = (
        datetime.now() - timedelta(days=random.randint(0, 1000))
    ).strftime("%Y%m%d%H%M%S")
    ET.SubElement(record, "STATUS_OF_INTERACTION").text = random.choice(
        ["INTERACTION COMPLETED", "CUSTOMER DROPPED", "TRANSFERRED"]
    )
    ET.SubElement(record, "TYPE_OF_INTERACTION").text = random.choice(["CALL", "CHAT"])
    ET.SubElement(record, "CUSTOMER_TYPE").text = random.choice(customer_types)
    ET.SubElement(record, "CONTACT_DURATION").text = str(timedelta(seconds=contact_duration_seconds))
    ET.SubElement(record, "AFTER_CONTACT_WORK_TIME").text = str(
        timedelta(seconds=after_contact_work_time_seconds)
    )
    ET.SubElement(record, "INCIDENT_STATUS").text = random.choice(
        [
            "RESOLVED", "PENDING RESOLUTION", "PENDING CUSTOMER UPDATE",
            "WORK IN PROGRESS", "TRANSFERRED TO ANOTHER QUEUE"
        ]
    )
    ET.SubElement(record, "FIRST_CONTACT_SOLVE").text = random.choice(["TRUE", "FALSE"])
    ET.SubElement(record, "TYPE_OF_RESOLUTION").text = random.choice(
        ["SELF-HELP OPTION", "SUPPORT TEAM INTERVENTION"]
    )
    ET.SubElement(record, "SUPPORT_RATING").text = str(random.randint(1, 5))
    ET.SubElement(record, "TIME_STAMP").text = datetime.now().strftime("%Y/%m/%d %H:%M:%S")

    logger.debug("Generated record with SERIAL_NUMBER: %d, SUPPORT_IDENTIFIER: %d", serial_number, record_id)
    return record

def generate_and_update_records(
    support_categories: List[str],
    agent_pseudo_names: List[str],
    customer_types: List[str],
    start_serial_number: int,
    start_record_id: int,
    num_records: int,
    producer: Producer,
    topic: str,
    s3_client: boto3.client,
    bucket_name: str,
    file_name: str
) -> None:
    """Generate and send records to Kafka, then update MinIO with highest values."""
    serial_number = start_serial_number
    record_id = start_record_id

    logger.debug("Generating %d new records", num_records)
    for _ in range(num_records):
        serial_number += 1
        record_id += 1
        new_record = generate_random_record(
            serial_number, record_id, support_categories, agent_pseudo_names, customer_types
        )

        if random.random() < 0.1:  # 10% chance of a NULL value
            key_to_nullify = random.choice(
                [
                    "CONTACT_REGARDING", "AGENT_CODE", "DATE_OF_INTERACTION",
                    "STATUS_OF_INTERACTION", "TYPE_OF_INTERACTION", "CUSTOMER_TYPE",
                    "CONTACT_DURATION", "AFTER_CONTACT_WORK_TIME", "INCIDENT_STATUS",
                    "FIRST_CONTACT_SOLVE", "SUPPORT_RATING"
                ]
            )
            new_record.find(key_to_nullify).text = None
            logger.debug("Set %s to None in record %d-%d", key_to_nullify, serial_number, record_id)

        produce_record_to_kafka(producer, topic, new_record)

    for current_record_id in range(start_record_id + 1, start_record_id + num_records + 1):
        if random.random() < 0.25:  # 25% chance of updating an existing record
            serial_number += 1
            update_record = generate_random_record(
                serial_number, current_record_id, support_categories, agent_pseudo_names, customer_types
            )
            produce_record_to_kafka(producer, topic, update_record)
            logger.debug("Updated record with SUPPORT_IDENTIFIER: %d", current_record_id)

    producer.flush()
    logger.info("Flushed %d records to Kafka topic %s", num_records, topic)

    write_highest_values_to_s3(s3_client, bucket_name, file_name, serial_number, record_id)

def main() -> None:
    """Main function to generate and send random Amazon support records to Kafka."""
    logger.info("Starting Kafka data generation process")

    # Fetch configuration from environment variables
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    kafka_topic_name = os.getenv("KAFKA_TOPIC_NAME")
    minio_host = os.getenv("MINIO_HOST")
    minio_port = os.getenv("MINIO_PORT")
    minio_user = os.getenv("MINIO_USER")
    minio_password = os.getenv("MINIO_PASSWORD")
    minio_bucket = os.getenv("MINIO_BUCKET")

    # Validate environment variables
    required_vars = {
        "DB_HOST": db_host,
        "DB_PORT": db_port,
        "DB_NAME": db_name,
        "DB_USER": db_user,
        "DB_PASSWORD": db_password,
        "KAFKA_BOOTSTRAP_SERVERS": kafka_bootstrap_servers,
        "KAFKA_TOPIC_NAME": kafka_topic_name,
        "MINIO_HOST": minio_host,
        "MINIO_PORT": minio_port,
        "MINIO_USER": minio_user,
        "MINIO_PASSWORD": minio_password,
        "MINIO_BUCKET": minio_bucket
    }
    missing_vars = [key for key, value in required_vars.items() if not value]
    if missing_vars:
        logger.error("Missing required environment variables: %s", ", ".join(missing_vars))
        sys.exit(1)

    # Connect to MySQL and fetch allowed values
    try:
        engine = connect_to_database(
            db_host,
            db_port,
            db_name,
            db_user,
            db_password,
            logger=logger
        )
        support_categories = fetch_allowed_values(
            logger, engine, "CSD_SUPPORT_AREAS", "'AMAZON'", "SUPPORT_AREA_NAME"
        )
        agent_pseudo_names = fetch_allowed_values(
            logger, engine, "CSD_AGENTS", "'AMAZON'", "PSEUDO_CODE"
        )
        customer_types = fetch_allowed_values(
            logger, engine, "CSD_CUSTOMER_TYPES", "'AMAZON'", "CUSTOMER_TYPE_NAME"
        )
    except Exception as e:
        logger.error("Failed to fetch allowed values from MySQL: %s", str(e), exc_info=True)
        sys.exit(1)
    finally:
        close_database_connection(logger, engine)

    # Initialize S3 client and fetch initial values
    try:
        s3_client = get_s3_client()
        file_name = "highest_values.json"
        max_serial_number, max_record_id = get_initial_values_from_s3(s3_client, minio_bucket, file_name)
    except Exception as e:
        logger.error("Failed to initialize S3 client or fetch initial values: %s", str(e), exc_info=True)
        sys.exit(1)

    # Set up Kafka producer
    producer = None
    try:
        producer_conf = {'bootstrap.servers': kafka_bootstrap_servers}
        producer = Producer(producer_conf)
        logger.info("Initialized Kafka producer with bootstrap servers: %s", kafka_bootstrap_servers)
    except Exception as e:
        logger.error("Failed to initialize Kafka producer: %s", str(e), exc_info=True)
        sys.exit(1)

    # Parse and validate command-line arguments
    parser = argparse.ArgumentParser(description="Generate and send records to Kafka.")
    parser.add_argument(
        "input",
        nargs="?",
        default=None,
        type=int,
        help="Number of records to generate (default: random between 1 and 1000)"
    )
    args = parser.parse_args()
    num_records = args.input if args.input is not None else random.randint(1, 1000)
    if num_records <= 0:
        logger.error("Number of records must be a positive integer, got %d", num_records)
        sys.exit(1)
    logger.info("Generating %d new records", num_records)

    # Generate and send records
    try:
        generate_and_update_records(
            support_categories,
            agent_pseudo_names,
            customer_types,
            max_serial_number,
            max_record_id,
            num_records,
            producer,
            kafka_topic_name,
            s3_client,
            minio_bucket,
            file_name
        )
        logger.info("Sent records to Kafka topic '%s'", kafka_topic_name)
    except Exception as e:
        logger.error("Failed to generate or send records: %s", str(e), exc_info=True)
        if producer:
            producer.flush()
        sys.exit(1)

    # Ensure producer flushes any remaining messages
    if producer:
        producer.flush()

if __name__ == "__main__":
    main()