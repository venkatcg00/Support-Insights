import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import xml.etree.ElementTree as ET
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Global flag to stop stream on critical errors
stop_stream = False

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
        return 0

def parse_xml(xml_string):
    """Parse XML string to extract ARCHIVE_ID and STREAM_RECORD_ID."""
    try:
        root = ET.fromstring(xml_string)
        archive_id = int(root.find("SERIAL_NUMBER").text or 0)
        record_id = int(root.find("SUPPORT_IDENTIFIER").text or 0)
        return archive_id, record_id, xml_string
    except Exception as e:
        logger.error(f"XML parsing error: {e}")
        return None, None, None

def write_to_mysql_partition(rows, db_url):
    """Write rows to MySQL within a Spark partition."""
    global stop_stream
    try:
        engine = create_engine(db_url, poolclass=QueuePool, pool_size=2, max_overflow=5)
        with engine.connect() as conn:
            batch_size = 100
            batch = []
            for row in rows:
                archive_id = row["ARCHIVE_ID"]
                record_id = row["STREAM_RECORD_ID"]
                xml_data = row["STREAMING_DATA"].replace("'", "''")
                batch.append((archive_id, record_id, xml_data))
                
                if len(batch) >= batch_size:
                    sql_query = """
                    INSERT INTO STREAMING_DATA_ARCHIVES (ARCHIVE_ID, STREAM_RECORD_ID, STREAMING_DATA)
                    VALUES (:archive_id, :record_id, :xml_data)
                    ON DUPLICATE KEY UPDATE 
                    STREAM_RECORD_ID = VALUES(STREAM_RECORD_ID), 
                    STREAMING_DATA = VALUES(STREAMING_DATA)
                    """
                    conn.execute(text(sql_query), [
                        {"archive_id": a_id, "record_id": r_id, "xml_data": x_data}
                        for a_id, r_id, x_data in batch
                    ])
                    conn.commit()
                    batch = []

            if batch:
                sql_query = """
                INSERT INTO STREAMING_DATA_ARCHIVES (ARCHIVE_ID, STREAM_RECORD_ID, STREAMING_DATA)
                VALUES (:archive_id, :record_id, :xml_data)
                ON DUPLICATE KEY UPDATE 
                STREAM_RECORD_ID = VALUES(STREAM_RECORD_ID), 
                STREAMING_DATA = VALUES(STREAMING_DATA)
                """
                conn.execute(text(sql_query), [
                    {"archive_id": a_id, "record_id": r_id, "xml_data": x_data}
                    for a_id, r_id, x_data in batch
                ])
                conn.commit()
                logger.info(f"Wrote {len(batch)} records to MySQL")
    except Exception as e:
        logger.error(f"Failed to write to MySQL: {e}")
        stop_stream = True
        raise

def main():
    global stop_stream

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
        return

    # MySQL URL
    db_url = f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("KafkaStreamingToMySQL") \
        .master("Local[*]") \
        .getOrCreate()

    # Define schema for parsed XML
    schema = StructType([
        StructField("ARCHIVE_ID", IntegerType(), True),
        StructField("STREAM_RECORD_ID", IntegerType(), True),
        StructField("STREAMING_DATA", StringType(), True)
    ])

    # Determine Kafka starting offset
    starting_offset = "earliest" if get_highest_archive_id(db_url) == 0 else "latest"
    logger.info(f"Starting Kafka stream with offset: {starting_offset}")

    # Read from Kafka
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", starting_offset) \
        .load()

    # Convert Kafka value to string
    kafka_df = kafka_df.selectExpr("CAST(value AS STRING) as xml_data")

    # Parse XML using UDF
    parse_xml_udf = udf(parse_xml, schema)
    parsed_df = kafka_df.withColumn("parsed", parse_xml_udf(col("xml_data"))) \
        .select(
            col("parsed.ARCHIVE_ID").alias("ARCHIVE_ID"),
            col("parsed.STREAM_RECORD_ID").alias("STREAM_RECORD_ID"),
            col("parsed.STREAMING_DATA").alias("STREAMING_DATA")
        ) \
        .filter(col("ARCHIVE_ID").isNotNull())

    def write_to_mysql(batch_df, batch_id):
        """Process each micro-batch."""
        global stop_stream
        try:
            batch_df.foreachPartition(lambda rows: write_to_mysql_partition(rows, db_url))
            logger.info(f"Processed batch {batch_id}")
        except Exception as e:
            logger.error(f"Failed to process batch {batch_id}: {e}")
            stop_stream = True

    # Write stream to MySQL
    query = parsed_df \
        .writeStream \
        .foreachBatch(write_to_mysql) \
        .outputMode("append") \
        .trigger(processingTime="10 seconds") \
        .start()

    # Monitor stream
    while query.isActive and not stop_stream:
        query.awaitTermination(timeout=10)

    if stop_stream:
        logger.error("Stopping stream due to critical error")
        query.stop()

    spark.stop()
    logger.info("Spark streaming stopped")

if __name__ == "__main__":
    main()