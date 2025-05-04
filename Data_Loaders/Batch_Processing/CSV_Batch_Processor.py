from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    md5, concat_ws, col, concat, lit, when, to_timestamp, udf, coalesce, row_number, to_date, date_format
)
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from airflow.providers.mysql.hooks.mysql import MySqlHook
from .db_lookup import fetch_lookup_dataframe
from datetime import datetime

def initialize_data_load(dag_run_id: str, mysql_conn_id: str) -> dict:
    mysql_hook = MySqlHook(mysql_conn_id=mysql_conn_id)
    load_start = datetime.now()
    
    # Check if a record with the given DAG_ID already exists
    fetch_query = """
        SELECT DATA_LOAD_ID
        FROM CSD_DATA_LOADS
        WHERE DAG_ID = %s
    """
    existing_record = mysql_hook.get_first(fetch_query, parameters=(dag_run_id,))
    
    if existing_record:
        data_load_id = existing_record[0]
        update_query = """
            UPDATE CSD_DATA_LOADS
            SET LOAD_STATUS = %s, LOAD_START_DATE = %s
            WHERE DAG_ID = %s
        """
        mysql_hook.run(update_query, parameters=("RUNNING", load_start, dag_run_id))
    else:
        insert_query = """
            INSERT INTO CSD_DATA_LOADS (DAG_ID, LOAD_STATUS, LOAD_START_DATE)
            VALUES (%s, %s, %s)
        """
        mysql_hook.run(insert_query, parameters=(dag_run_id, "RUNNING", load_start))
        data_load_id = mysql_hook.get_first(fetch_query, parameters=(dag_run_id,))[0]
    
    return {"dag_run_id": dag_run_id, "data_load_id": data_load_id, "load_start": load_start}

def process_and_transform_csv(local_path: str, data_load_info: dict, mysql_conn_id: str, spark: SparkSession) -> dict:
    dag_run_id = data_load_info['dag_run_id']
    data_load_id = data_load_info['data_load_id']

    mysql_hook = MySqlHook(mysql_conn_id=mysql_conn_id)
    query = """
        SELECT SOURCE_ID
        FROM CSD_SOURCES
        WHERE SOURCE_NAME = 'AT&T' AND ACTIVE_FLAG = 1
        LIMIT 1
    """
    source_id = mysql_hook.get_first(query)[0]

    agents_df = fetch_lookup_dataframe(mysql_conn_id, 'CSD_AGENTS', source_id)
    support_areas_df = fetch_lookup_dataframe(mysql_conn_id, 'CSD_SUPPORT_AREAS', source_id)
    customer_types_df = fetch_lookup_dataframe(mysql_conn_id, 'CSD_CUSTOMER_TYPES', source_id)

    agents_dict = agents_df.set_index('PSEUDO_CODE')['AGENT_ID'].to_dict() if agents_df is not None else {}
    support_areas_dict = support_areas_df.set_index('SUPPORT_AREA_NAME')['SUPPORT_AREA_ID'].to_dict() if support_areas_df is not None else {}
    customer_types_dict = customer_types_df.set_index('CUSTOMER_TYPE_NAME')['CUSTOMER_TYPE_ID'].to_dict() if customer_types_df is not None else {}

    broadcast_agents = spark.sparkContext.broadcast(agents_dict)
    broadcast_support_areas = spark.sparkContext.broadcast(support_areas_dict)
    broadcast_customer_types = spark.sparkContext.broadcast(customer_types_dict)

    def get_agent_id(agent_name): 
        return broadcast_agents.value.get(agent_name)
    def get_support_area_id(support_area): 
        return broadcast_support_areas.value.get(support_area)
    def get_customer_type_id(customer_type): 
        return broadcast_customer_types.value.get(customer_type)

    get_agent_id_udf = udf(get_agent_id, StringType())
    get_support_area_id_udf = udf(get_support_area_id, StringType())
    get_customer_type_id_udf = udf(get_customer_type_id, StringType())

    # Define CSV schema with more permissive types to debug parsing issues
    csv_schema = StructType([
        StructField("TICKET_IDENTIFIER", IntegerType(), True),
        StructField("SUPPORT_CATEGORY", StringType(), True),
        StructField("AGENT_NAME", StringType(), True),
        StructField("DATE_OF_CALL", StringType(), True),
        StructField("CALL_STATUS", StringType(), True),
        StructField("CALL_TYPE", StringType(), True),
        StructField("TYPE_OF_CUSTOMER", StringType(), True),
        StructField("DURATION", StringType(), True),
        StructField("WORK_TIME", StringType(), True),
        StructField("TICKET_STATUS", StringType(), True),
        StructField("RESOLVED_IN_FIRST_CONTACT", StringType(), True),
        StructField("RESOLUTION_CATEGORY", StringType(), True),
        StructField("RATING", StringType(), True),
    ])
    csv_df = spark.read.csv(local_path, header=True, schema=csv_schema, sep="|")
    
    # Convert numeric columns to IntegerType after cleaning
    csv_df = csv_df.withColumn("DURATION", col("DURATION").cast(IntegerType())) \
                   .withColumn("WORK_TIME", col("WORK_TIME").cast(IntegerType())) \
                   .withColumn("RESOLVED_IN_FIRST_CONTACT", col("RESOLVED_IN_FIRST_CONTACT").cast(IntegerType()))

    # Clean RATING column to ensure valid values
    valid_ratings = ["WORST", "BAD", "NEUTRAL", "GOOD", "BEST"]
    csv_df = csv_df.withColumn("RATING", when(col("RATING").isin(valid_ratings), col("RATING")).otherwise(None))

    window_spec = Window.partitionBy("TICKET_IDENTIFIER").orderBy(col("TICKET_IDENTIFIER").desc())
    csv_df = csv_df.withColumn("row_num", row_number().over(window_spec)) \
                  .filter(col("row_num") == 1) \
                  .drop("row_num") \
                  .withColumn("HASHKEY", md5(concat_ws("||", *[coalesce(col(c), lit("NULL")) for c in csv_df.columns]))) \
                  .withColumn("TICKET_IDENTIFIER", concat(lit("AT&T - "), col("TICKET_IDENTIFIER")))

    historical_query = """
        SELECT CSD_ID AS HISTORIC_CSD_ID, SOURCE_SYSTEM_IDENTIFIER AS HISTORIC_SSI, SOURCE_HASH_KEY AS HISTORIC_HASHKEY
        FROM CSD_DATA_MART
        WHERE ACTIVE_FLAG = 1 AND SOURCE_ID = %s
    """
    historical_pd = mysql_hook.get_pandas_df(historical_query, parameters=(source_id,))
    historical_schema = StructType([
        StructField("HISTORIC_CSD_ID", IntegerType(), True),
        StructField("HISTORIC_SSI", StringType(), True),
        StructField("HISTORIC_HASHKEY", StringType(), True),
    ])
    historical_df = spark.createDataFrame(historical_pd, historical_schema) if not historical_pd.empty else spark.createDataFrame([], historical_schema)

    df = csv_df.join(historical_df, csv_df["TICKET_IDENTIFIER"] == historical_df["HISTORIC_SSI"], "left")
    router_df = df.withColumn(
        "ROUTER_GROUP",
        when(col("HISTORIC_SSI").isNull(), "INSERT")
        .when(col("HASHKEY") == col("HISTORIC_HASHKEY"), "DUPLICATE")
        .otherwise("UPDATE")
    )
    filter_df = router_df.filter(col("ROUTER_GROUP") != "DUPLICATE")
    transformed_df = filter_df.withColumn("SOURCE_ID", lit(source_id)) \
                             .withColumn("SOURCE_SYSTEM_IDENTIFIER", col("TICKET_IDENTIFIER")) \
                             .withColumn("AGENT_ID", get_agent_id_udf(col("AGENT_NAME"))) \
                             .withColumn("INTERACTION_DATE", date_format(to_timestamp(col("DATE_OF_CALL"), "MMddyyyyHHmmss"), "yyyy-MM-dd HH:mm:ss")) \
                             .withColumn("SUPPORT_AREA_ID", get_support_area_id_udf(col("SUPPORT_CATEGORY"))) \
                             .withColumn("INTERACTION_STATUS", col("CALL_STATUS")) \
                             .withColumn("INTERACTION_TYPE", col("CALL_TYPE")) \
                             .withColumn("CUSTOMER_TYPE_ID", get_customer_type_id_udf(col("TYPE_OF_CUSTOMER"))) \
                             .withColumn("HANDLE_TIME", col("DURATION")) \
                             .withColumn("WORK_TIME", col("WORK_TIME")) \
                             .withColumn("FIRST_CONTACT_RESOLUTION", col("RESOLVED_IN_FIRST_CONTACT")) \
                             .withColumn("QUERY_STATUS", col("TICKET_STATUS")) \
                             .withColumn("SOLUTION_TYPE", col("RESOLUTION_CATEGORY")) \
                             .withColumn("CUSTOMER_RATING", when(col("RATING") == "WORST", 1) \
                                                        .when(col("RATING") == "BAD", 2) \
                                                        .when(col("RATING") == "NEUTRAL", 3) \
                                                        .when(col("RATING") == "GOOD", 4) \
                                                        .when(col("RATING") == "BEST", 5).otherwise(None)) \
                             .withColumn("SOURCE_HASH_KEY", col("HASHKEY")) \
                             .withColumn("HISTORIC_CSD_ID", col("HISTORIC_CSD_ID")) \
                             .withColumn("DATA_LOAD_ID", lit(data_load_id)) \
                             .withColumn("START_DATE", date_format(lit(datetime.now()), "yyyy-MM-dd HH:mm:ss")) \
                             .withColumn("END_DATE", date_format(to_date(lit("2099-12-31"), "yyyy-MM-dd"), "yyyy-MM-dd")) \
                             .withColumn("IS_VALID_DATA", when(
                                 col("AGENT_ID").isNull() |
                                 col("INTERACTION_DATE").isNull() |
                                 col("SUPPORT_AREA_ID").isNull() |
                                 col("INTERACTION_STATUS").isNull() |
                                 col("INTERACTION_TYPE").isNull() |
                                 col("CUSTOMER_TYPE_ID").isNull() |
                                 col("HANDLE_TIME").isNull() |
                                 col("WORK_TIME").isNull() |
                                 col("FIRST_CONTACT_RESOLUTION").isNull() |
                                 col("QUERY_STATUS").isNull() |
                                 col("SOLUTION_TYPE").isNull() |
                                 col("CUSTOMER_RATING").isNull(), 0).otherwise(1))

    transformed_df = transformed_df.select(
        "SOURCE_ID", "SOURCE_SYSTEM_IDENTIFIER", "AGENT_ID", "INTERACTION_DATE",
        "SUPPORT_AREA_ID", "INTERACTION_STATUS", "INTERACTION_TYPE", "CUSTOMER_TYPE_ID",
        "HANDLE_TIME", "WORK_TIME", "FIRST_CONTACT_RESOLUTION", "QUERY_STATUS",
        "SOLUTION_TYPE", "CUSTOMER_RATING", "SOURCE_HASH_KEY", "IS_VALID_DATA",
        "HISTORIC_CSD_ID", "ROUTER_GROUP", "DATA_LOAD_ID", "START_DATE", "END_DATE"
    )

    return {"dag_run_id": dag_run_id, "data_load_id": data_load_id, "transformed_df": transformed_df}

def upsert_data(transformed_info: dict, mysql_conn_id: str, spark: SparkSession) -> dict:
    mysql_hook = MySqlHook(mysql_conn_id=mysql_conn_id)
    dag_run_id = transformed_info['dag_run_id']
    data_load_id = transformed_info['data_load_id']
    transformed_df = transformed_info['transformed_df']

    total_rows = transformed_df.count()
    duplicate_count = transformed_df.filter(col("ROUTER_GROUP") == "DUPLICATE").count()
    upsert_df = transformed_df.filter(col("ROUTER_GROUP") != "DUPLICATE")
    valid_count = upsert_df.filter(col("IS_VALID_DATA") == 1).count()
    invalid_count = upsert_df.filter(col("IS_VALID_DATA") == 0).count()
    update_df = upsert_df.filter(col("ROUTER_GROUP") == "UPDATE")
    update_count = update_df.count()
    insert_count = upsert_df.filter(col("ROUTER_GROUP") == "INSERT").count()
    total_upsert_count = update_count + insert_count

    if update_count > 0:
        update_pd = update_df.select("HISTORIC_CSD_ID", "START_DATE").toPandas()
        update_rows = []
        for _, row in update_pd.iterrows():
            row_dict = row.to_dict()
            # START_DATE is already a string in the correct format
            row_dict["HISTORIC_CSD_ID"] = row_dict["HISTORIC_CSD_ID"] if row_dict["HISTORIC_CSD_ID"] is not None else None
            update_rows.append((row_dict["HISTORIC_CSD_ID"], row_dict["START_DATE"]))
        deactivate_query = """
            UPDATE CSD_DATA_MART
            SET ACTIVE_FLAG = 0, END_DATE = %s
            WHERE CSD_ID = %s AND ACTIVE_FLAG = 1
        """
        mysql_hook.insert_rows(
            table="CSD_DATA_MART",
            rows=update_rows,
            target_fields=["END_DATE", "CSD_ID"],
            replace=True,
            commit_every=1000,
            execute_query=deactivate_query
        )

    if total_upsert_count > 0:
        insert_pd = upsert_df.select(
            "SOURCE_SYSTEM_IDENTIFIER", "SOURCE_HASH_KEY", "SOURCE_ID", "AGENT_ID",
            "INTERACTION_DATE", "SUPPORT_AREA_ID", "INTERACTION_STATUS", "INTERACTION_TYPE",
            "CUSTOMER_TYPE_ID", "HANDLE_TIME", "WORK_TIME", "FIRST_CONTACT_RESOLUTION",
            "QUERY_STATUS", "SOLUTION_TYPE", "CUSTOMER_RATING", "DATA_LOAD_ID",
            "IS_VALID_DATA", lit(1).alias("ACTIVE_FLAG"), "START_DATE", "END_DATE"
        ).toPandas()
        insert_rows = []
        for _, row in insert_pd.iterrows():
            row_dict = row.to_dict()
            # START_DATE is already a string in the correct format
            # Explicitly handle numeric columns to avoid NaN
            numeric_cols = ["SOURCE_ID", "HANDLE_TIME", "WORK_TIME", "FIRST_CONTACT_RESOLUTION", "CUSTOMER_RATING", "DATA_LOAD_ID", "IS_VALID_DATA", "ACTIVE_FLAG"]
            for col_name in numeric_cols:
                row_dict[col_name] = row_dict[col_name] if row_dict[col_name] is not None else None
            # Map to the expected order for insertion
            insert_rows.append((
                row_dict["SOURCE_SYSTEM_IDENTIFIER"],
                row_dict["SOURCE_HASH_KEY"],
                row_dict["SOURCE_ID"],
                row_dict["AGENT_ID"],
                row_dict["INTERACTION_DATE"],
                row_dict["SUPPORT_AREA_ID"],
                row_dict["INTERACTION_STATUS"],
                row_dict["INTERACTION_TYPE"],
                row_dict["CUSTOMER_TYPE_ID"],
                row_dict["HANDLE_TIME"],
                row_dict["WORK_TIME"],
                row_dict["FIRST_CONTACT_RESOLUTION"],
                row_dict["QUERY_STATUS"],
                row_dict["SOLUTION_TYPE"],
                row_dict["CUSTOMER_RATING"],
                row_dict["DATA_LOAD_ID"],
                row_dict["IS_VALID_DATA"],
                row_dict["ACTIVE_FLAG"],
                row_dict["START_DATE"],
                row_dict["END_DATE"]
            ))
        mysql_hook.insert_rows(
            table="CSD_DATA_MART",
            rows=insert_rows,
            target_fields=[
                "SOURCE_SYSTEM_IDENTIFIER", "SOURCE_HASH_KEY", "SOURCE_ID", "AGENT_ID",
                "INTERACTION_DATE", "SUPPORT_AREA_ID", "INTERACTION_STATUS", "INTERACTION_TYPE",
                "CUSTOMER_TYPE_ID", "HANDLE_TIME", "WORK_TIME", "FIRST_CONTACT_RESOLUTION",
                "QUERY_STATUS", "SOLUTION_TYPE", "CUSTOMER_RATING", "DATA_LOAD_ID",
                "IS_VALID_DATA", "ACTIVE_FLAG", "START_DATE", "END_DATE"
            ],
            commit_every=1000
        )

    load_end = datetime.now()
    load_start = mysql_hook.get_first("SELECT LOAD_START_DATE FROM CSD_DATA_LOADS WHERE DAG_ID = %s", (dag_run_id,))[0]
    load_duration = (load_end - load_start).total_seconds()
    data_valid_percentage = (valid_count / total_upsert_count * 100) if total_upsert_count > 0 else 0
    update_query = """
        UPDATE CSD_DATA_LOADS
        SET LOAD_STATUS = %s, LOAD_END_DATE = %s, BATCH_COUNT = %s, INSERT_COUNT = %s,
            UPDATE_COUNT = %s, DUPLICATE_COUNT = %s, UPSERT_COUNT = %s, VALID_COUNT = %s,
            INVALID_COUNT = %s, DATA_VALID_PERCENTAGE = %s, LOAD_DURATION = %s
        WHERE DAG_ID = %s
    """
    mysql_hook.run(update_query, parameters=(
        "SUCCESS", load_end, total_rows, insert_count, update_count, duplicate_count,
        total_upsert_count, valid_count, invalid_count, data_valid_percentage,
        f"{load_duration:.2f} seconds", dag_run_id
    ))

    return {"status": "completed"}

def whole_etl_process(dag_run_id: str, merged_path: str, mysql_conn_id: str) -> dict:
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("CSV Batch Processing") \
        .getOrCreate()
    
    try:
        # Step 1: Initialize data load
        data_load_info = initialize_data_load(dag_run_id, mysql_conn_id)
        
        # Step 2: Process and transform CSV
        transformed_info = process_and_transform_csv(merged_path, data_load_info, mysql_conn_id, spark)
        transformed_info['merged_path'] = merged_path  # Pass merged_path for upsert
        
        # Step 3: Upsert data
        result = upsert_data(transformed_info, mysql_conn_id, spark)
        
        return result
    except Exception:
        raise
    finally:
        try:
            spark.stop()
        except Exception:
            pass