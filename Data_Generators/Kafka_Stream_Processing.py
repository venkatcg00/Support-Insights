import os
import logging
import xml.etree.ElementTree as ET
from confluent_kafka import Consumer, KafkaError, KafkaException
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool
import traceback

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_highest_archive_id(db_url):
    """Get the highest ARCHIVE_ID from STREAMING_DATA_ARCHIVES table."""
    try:
        engine = create_engine(db_url, poolclass=QueuePool, pool_size=5, max_overflow=10)
        with engine.connect() as conn:
            result = conn.execute(text("SELECT MAX(ARCHIVE_ID) FROM STREAMING_DATA_ARCHIVES")).scalar()
            highest_archive_id = result if result is not None else 0
            logger.info(f"Highest ARCHIVE_ID: {highest_archive_id}")
            return highest_archive_id
    except Exception as e:
        logger.error(f"Failed to retrieve ARCHIVE_ID: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return 0

def parse_xml(xml_string):
    """Parse XML string to extract ARCHIVE_ID and STREAM_RECORD_ID."""
    try:
        root = ET.fromstring(xml_string)
        archive_id = int(root.find("SERIAL_NUMBER").text or 0)
        record_id = int(root.find("SUPPORT_IDENTIFIER").text or 0)
        logger.debug(f"Parsed XML: archive_id={archive_id}, record_id={record_id}")
        return archive_id, record_id, xml_string
    except Exception as e:
        logger.error(f"XML parsing error: {e}, XML: {xml_string}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return None, None, None

def write_to_mysql(archive_id, record_id, xml_data, db_url):
    """Write a single record to MySQL, updating if ARCHIVE_ID exists."""
    try:
        engine = create_engine(db_url, poolclass=QueuePool, pool_size=2, max_overflow=5)
        with engine.connect() as conn:
            sql_query = """
            INSERT INTO STREAMING_DATA_ARCHIVES (ARCHIVE_ID, STREAM_RECORD_ID, STREAMING_DATA)
            VALUES (:archive_id, :record_id, :xml_data)
            ON DUPLICATE KEY UPDATE 
            STREAM_RECORD_ID = VALUES(STREAM_RECORD_ID), 
            STREAMING_DATA = VALUES(STREAMING_DATA)
            """
            conn.execute(text(sql_query), {
                "archive_id": archive_id,
                "record_id": record_id,
                "xml_data": xml_data
            })
            conn.commit()
            logger.info(f"Wrote ARCHIVE_ID {archive_id} to MySQL")
    except Exception as e:
        logger.error(f"Failed to write ARCHIVE_ID {archive_id} to MySQL: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise

def main():
    # Fetch environment variables
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    kafka_topic = os.getenv("KAFKA_TOPIC_NAME")

    # Validate environment variables
    required_vars = {
        "DB_HOST": db_host, "DB_PORT": db_port, "DB_NAME": db_name,
        "DB_USER": db_user, "DB_PASSWORD": db_password,
        "KAFKA_BOOTSTRAP_SERVERS": kafka_bootstrap_servers,
        "KAFKA_TOPIC_NAME": kafka_topic
    }
    missing_vars = [key for key, value in required_vars.items() if not value]
    if missing_vars:
        logger.error(f"Missing environment variables: {', '.join(missing_vars)}")
        return 1

    # MySQL URL
    db_url = f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

    # Get the highest ARCHIVE_ID to determine starting point
    last_archive_id = get_highest_archive_id(db_url)
    if last_archive_id is None:
        logger.error("Could not determine last ARCHIVE_ID. Exiting.")
        return 1

    # Initialize Kafka consumer
    try:
        consumer = Consumer({
            'bootstrap.servers': kafka_bootstrap_servers,
            'group.id': 'spark_streaming_consumer',
            'auto.offset.reset': 'earliest',  # Read all messages, filter by ARCHIVE_ID
            'enable.auto.commit': 'false',
        })
        consumer.subscribe([kafka_topic])
        logger.info(f"Connected to Kafka topic {kafka_topic} with bootstrap servers {kafka_bootstrap_servers}")
    except KafkaException as e:
        logger.error(f"Failed to connect to Kafka: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return 1

    # Process Kafka messages
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                logger.debug("No message received")
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.info("Reached partition EOF")
                    continue
                else:
                    logger.error(f"Kafka consumer error: {msg.error()}")
                    return 1

            xml_string = msg.value().decode('utf-8')
            archive_id, record_id, xml_data = parse_xml(xml_string)
            if archive_id is None or record_id is None:
                logger.warning(f"Skipping message due to parsing error: {xml_string}")
                continue

            # Skip messages with ARCHIVE_ID less than or equal to last_archive_id
            if archive_id <= last_archive_id:
                logger.debug(f"Skipping ARCHIVE_ID {archive_id} <= last_archive_id {last_archive_id}")
                continue

            # Write record immediately
            xml_data = xml_data.replace("'", "''")
            write_to_mysql(archive_id, record_id, xml_data, db_url)
            last_archive_id = archive_id

    except Exception as e:
        logger.error(f"Error processing Kafka messages: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return 1
    finally:
        consumer.close()
        logger.info("Kafka consumer closed")

    return 0

if __name__ == "__main__":
    exit_code = main()
    if exit_code is not None:
        exit(exit_code)