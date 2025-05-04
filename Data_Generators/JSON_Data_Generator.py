"""Data generator script for creating and loading Amazon support records into MongoDB.

This script generates random support records based on allowed values from a MySQL database,
fetches the maximum serial number and interaction ID from MongoDB, and inserts or updates
records in a specified MongoDB collection. It supports configurable record counts via
command-line arguments and is designed to run under Data_Generator_Orchestrator.py with a
log file specified via the SCRIPT_LOG_FILE environment variable. All configuration is sourced
from environment variables.

Example (run inside python-runner container):
    $ python /app/scripts/JSON_Data_Generator.py 500
    # Generates 500 records and loads them into the configured MongoDB collection.
"""

import random
import logging
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
import argparse
from pymongo import MongoClient
from pymongo.collection import Collection
from allowed_values import connect_to_database, fetch_allowed_values, close_database_connection
import os
import sys

# Use log file from environment variable set by orchestrator, with fallback
log_file_name = os.getenv("SCRIPT_LOG_FILE", f"/app/logs/JSON_Data_Generator_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file_name),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def get_mongodb_collection(
    db_name: str,
    collection_name: str,
    client: MongoClient
) -> Collection:
    """Return the MongoDB collection object."""
    logger.debug("Accessing collection %s in database %s", collection_name, db_name)
    db = client[db_name]
    return db[collection_name]

def get_max_values(collection: Collection) -> Tuple[int, int]:
    """Fetch the maximum serial_number and interaction_id from MongoDB."""
    logger.debug("Fetching max serial_number and interaction_id from collection")
    try:
        max_serial_doc = collection.find({}, {"serial_number": 1}).sort("serial_number", -1).limit(1)
        max_interaction_doc = collection.find({}, {"record_id": 1}).sort("record_id", -1).limit(1)

        max_serial_list = list(max_serial_doc)
        max_interaction_list = list(max_interaction_doc)

        max_serial = max_serial_list[0]["serial_number"] if max_serial_list else 0
        max_interaction = max_interaction_list[0]["record_id"] if max_interaction_list else 0

        logger.debug("Max serial_number: %d, Max interaction_id: %d", max_serial, max_interaction)
        return max_serial, max_interaction
    except Exception as e:
        logger.error("Error fetching max values from MongoDB: %s", str(e), exc_info=True)
        return 0, 0

def insert_record(
    collection: Collection,
    serial_number: int,
    interaction_id: int,
    record: Dict[str, Optional[Any]]
) -> None:
    """Insert or update a record in MongoDB."""
    record["_id"] = f"{serial_number}-{interaction_id}"
    record["serial_number"] = serial_number
    record["record_id"] = interaction_id
    try:
        collection.update_one(
            {"_id": record["_id"]},
            {"$set": record},
            upsert=True
        )
        logger.debug("Inserted/updated record with _id: %s", record["_id"])
    except Exception as e:
        logger.error("Failed to insert/update record with _id %s: %s", record["_id"], str(e), exc_info=True)
        raise

def generate_random_record(
    interaction_id: int,
    support_categories: List[str],
    agent_pseudo_names: List[str],
    customer_types: List[str]
) -> Dict[str, Optional[Any]]:
    """Generate a random record with specified data fields."""
    interaction_duration = random.randint(10, 600)
    record = {
        "INTERACTION_ID": interaction_id,
        "SUPPORT_CATEGORY": random.choice(support_categories),
        "AGENT_PSEUDO_NAME": random.choice(agent_pseudo_names),
        "CONTACT_DATE": (
            datetime.now() - timedelta(days=random.randint(0, 1000))
        ).strftime("%d/%m/%Y %H:%M:%S"),
        "INTERACTION_STATUS": random.choice(["COMPLETED", "DROPPED", "TRANSFERRED"]),
        "INTERACTION_TYPE": random.choice(["CALL", "CHAT"]),
        "TYPE_OF_CUSTOMER": random.choice(customer_types),
        "INTERACTION_DURATION": interaction_duration,
        "TOTAL_TIME": interaction_duration + random.randint(10, 600),
        "STATUS_OF_CUSTOMER_INCIDENT": random.choice(
            [
                "RESOLVED",
                "PENDING RESOLUTION",
                "PENDING CUSTOMER UPDATE",
                "WORK IN PROGRESS",
                "TRANSFERRED TO ANOTHER QUEUE",
            ]
        ),
        "RESOLVED_IN_FIRST_CONTACT": random.choice(["YES", "NO"]),
        "SOLUTION_TYPE": random.choice(["SELF-HELP OPTION", "SUPPORT TEAM INTERVENTION"]),
        "RATING": random.randint(1, 10),
    }
    logger.debug("Generated record with INTERACTION_ID: %d", interaction_id)
    return record

def generate_and_update_records(
    support_categories: List[str],
    agent_pseudo_names: List[str],
    customer_types: List[str],
    start_serial_number: int,
    start_interaction_id: int,
    num_records: int,
    collection: Collection
) -> None:
    """Generate and load records into MongoDB."""
    serial_number = start_serial_number
    interaction_id = start_interaction_id

    for _ in range(num_records):
        serial_number += 1
        interaction_id += 1
        new_record = generate_random_record(
            interaction_id, support_categories, agent_pseudo_names, customer_types
        )

        if random.random() < 0.1:  # 10% chance of a NULL value
            key_to_nullify = random.choice(
                [
                    "SUPPORT_CATEGORY", "AGENT_PSEUDO_NAME", "CONTACT_DATE",
                    "INTERACTION_STATUS", "INTERACTION_TYPE", "TYPE_OF_CUSTOMER",
                    "INTERACTION_DURATION", "TOTAL_TIME", "STATUS_OF_CUSTOMER_INCIDENT",
                    "SOLUTION_TYPE", "RATING"
                ]
            )
            new_record[key_to_nullify] = None
            logger.debug("Set %s to None in record with INTERACTION_ID: %d", key_to_nullify, interaction_id)

        insert_record(collection, serial_number, interaction_id, new_record)

        if random.random() < 0.25 and interaction_id > 1:  # 25% chance of updating an existing record
            serial_number += 1
            update_interaction_id = random.randint(1, interaction_id)
            update_record = generate_random_record(
                update_interaction_id, support_categories, agent_pseudo_names, customer_types
            )
            insert_record(collection, serial_number, update_interaction_id, update_record)
            logger.debug("Updated record with INTERACTION_ID: %d", update_interaction_id)

    logger.info("Generated and loaded %d records into MongoDB", num_records)

def main() -> None:
    """Main function to generate and load random Amazon support records into MongoDB."""
    logger.info("Starting MongoDB data generation process")

    # Fetch configuration from environment variables
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    mongo_host = os.getenv("MONGO_HOST")
    mongo_port = os.getenv("MONGO_PORT")
    mongo_db = os.getenv("MONGO_DB")
    mongo_user = os.getenv("MONGO_USER")
    mongo_password = os.getenv("MONGO_PASSWORD")
    mongo_collection_name = os.getenv("MONGO_COLLECTION_NAME", "amazon_support_records")

    # Validate environment variables
    required_vars = {
        "DB_HOST": db_host,
        "DB_PORT": db_port,
        "DB_NAME": db_name,
        "DB_USER": db_user,
        "DB_PASSWORD": db_password,
        "MONGO_HOST": mongo_host,
        "MONGO_PORT": mongo_port,
        "MONGO_DB": mongo_db,
        "MONGO_USER": mongo_user,
        "MONGO_PASSWORD": mongo_password
    }
    missing_vars = [key for key, value in required_vars.items() if not value]
    if missing_vars:
        logger.error("Missing required environment variables: %s", ", ".join(missing_vars))
        sys.exit(1)

    # Connect to MongoDB
    client = None
    try:
        client = MongoClient(
            host=mongo_host,
            port=int(mongo_port),
            username=mongo_user,
            password=mongo_password,
            authSource="admin"
        )
        collection = get_mongodb_collection(mongo_db, mongo_collection_name, client)
        logger.info("Connected to MongoDB: %s:%s/%s", mongo_host, mongo_port, mongo_db)
    except Exception as e:
        logger.error("Failed to connect to MongoDB: %s", str(e), exc_info=True)
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

    # Fetch max values from MongoDB
    try:
        max_serial_number, max_interaction_id = get_max_values(collection)
    except Exception as e:
        logger.error("Failed to fetch max values from MongoDB: %s", str(e), exc_info=True)
        sys.exit(1)

    # Parse and validate command-line arguments
    parser = argparse.ArgumentParser(description="Generate and load records into MongoDB.")
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

    # Generate and load records
    try:
        generate_and_update_records(
            support_categories,
            agent_pseudo_names,
            customer_types,
            max_serial_number,
            max_interaction_id,
            num_records,
            collection
        )
    except Exception as e:
        logger.error("Failed to generate or load records: %s", str(e), exc_info=True)
        sys.exit(1)

    # Close MongoDB connection
    if client:
        client.close()
        logger.info("MongoDB connection closed. Process completed")

if __name__ == "__main__":
    main()