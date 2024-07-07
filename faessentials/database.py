import asyncio
import json
import os
import random
import re
from enum import Enum
from typing import List
import httpx
import pydantic
from aiokafka.errors import KafkaError
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from faessentials import utils, global_logger
from faessentials.constants import DEFAULT_ENCODING, DEFAULT_CONNECTION_TIMEOUT

logger = global_logger.setup_custom_logger("app")

class KSQLNotReadyError(Exception):
    pass

class KafkaKSqlDbEndPoint(str, Enum):
    KSQL = "ksql"
    KSQL_TERMINATE = "ksql/terminate"
    QUERY = "query"
    QUERY_STREAM = "query-stream"
    STATUS = "status"
    INFO = "info"
    CLUSTER_STATUS = "clusterStatus"
    IS_VALID_PROPERTY = "is_valid_property"

async def is_kafka_available() -> bool:
    """
    Check if the Kafka brokers are available.

    :param brokers: A string of Kafka brokers (e.g., 'localhost:9092')
    :return: True if available, False otherwise
    """
    try:
        BROKERS = get_kafka_cluster_brokers()
        producer = AIOKafkaProducer(bootstrap_servers=BROKERS)
        await producer.start()
        await producer.stop()
        return True
    except Exception as e:
        logger.error(f"Error checking Kafka availability: {e}")
        return False

def get_kafka_cluster_brokers() -> List[str]:
    """Fetch the kafka broker array. This should return an array with nodes and ports.
    e.g. ['localhost:9092', 'localhost:9093']"""
    if utils.get_environment().upper() in ["DEV", None]:
        kafka_broker_string: str = os.getenv("KAFKA_BROKER_STRING", "NODES_NOT_DEFINED")
        if kafka_broker_string == "NODES_NOT_DEFINED":
            return ['localhost:9092']
        else:
            return kafka_broker_string
    else:
        # the value of the KAFKA_BROKER_STRING is set by the global config map.
        brokers = os.getenv("KAFKA_BROKER_STRING", "NODES_NOT_DEFINED")
    return brokers.split(",")

def compose_consumer_id() -> str:
    """Do not mistaken the consumer_id for the consumer_group_name.
    The consumer group is unique for a group of consumers - since it is expected to be the kubernetes pod name.
    Where as the consumer_id - what this functin returns - is a globally unique identifier.
    As of now, regard the consumer_id and consumer_name the same."""
    return utils.get_pod_name()

def compose_consumer_group_name() -> str:
    """Do not mistaken the consumer_group_name for the consumer_id.
    The consumer group name - what this function returns - is unique for a group of consumers.
    Where as the consumer_id is a globally unique identifier.
    The consumer_group_name is composed of domain and application name."""
    return utils.get_application_identifier()

async def topic_exists(topic_name):
    consumer = AIOKafkaConsumer(bootstrap_servers=get_kafka_cluster_brokers())
    await consumer.start()
    try:
        return topic_name in await consumer.topics()
    finally:
        await consumer.stop()

def compose_producer_id() -> str:
    """Creates a unique producer id: the pod_name."""
    return utils.get_pod_name()

async def get_default_kafka_producer(client_id: str = compose_producer_id()) -> AIOKafkaProducer:
    """ Caution: Always stop/close this producer when done.
        This default producer is expecting you to send json data, which it will then automatically
        serialize/encode with UTF-8.
        The key must be a posix timestamp (int in python, BIGINT in Kafka).
        Feel free to create your own kafka producer, if these default values do no suite the use case.
    """
    brokers: List[str] = get_kafka_cluster_brokers()
    broker_str = ",".join(brokers)

    def get_value_serializer(v: any) -> bytes:
        if isinstance(v, pydantic.BaseModel):
            # Pydantic models need different deserialization
            return v.model_dump_json().encode(DEFAULT_ENCODING)
        else:
            return json.dumps(v).encode(DEFAULT_ENCODING)

    def get_key_serializer(k: str) -> bytes:
        return k.encode(DEFAULT_ENCODING)    

    producer: AIOKafkaProducer = AIOKafkaProducer(
        bootstrap_servers=broker_str,
        client_id=client_id,
        key_serializer=lambda k: get_key_serializer(k),
        value_serializer=lambda v: get_value_serializer(v))

    # start the producer for the client (it is often forgotten).
    await producer.start()
    return producer

async def get_default_kafka_consumer(topics: str, client: str = compose_consumer_id(), consumer_group: str = None, auto_commit: bool = True, auto_offset_reset='latest') -> AIOKafkaConsumer:
    """ Will return an async-capable consumer.
        However, you may create your own consumer with specific settings. This is only for convenience.
        The offset could be set to 'earliest'. Default is 'latest'.
        : param auto_commit (True): Set auto_commit to False to control the commits yourself.
    """

    def get_key_deserializer(key_bytes: bytes) -> str:
        return key_bytes.decode(DEFAULT_ENCODING)
        
    brokers: List[str] = get_kafka_cluster_brokers()
    broker_str = ",".join(brokers)
    # Create the Producer instance
    consumer: AIOKafkaConsumer = AIOKafkaConsumer(topics,
                                                  bootstrap_servers=broker_str,
                                                  client_id=client,
                                                  group_id=consumer_group,
                                                  key_deserializer=lambda k: get_key_deserializer(k),
                                                  value_deserializer=lambda v: json.loads(v.decode(DEFAULT_ENCODING)),
                                                  auto_offset_reset=auto_offset_reset,
                                                  enable_auto_commit=auto_commit)
    await consumer.start()
    return consumer

def bytes_to_int_big_endian(key_bytes: bytes) -> int or None:
    """Converts 8 bytes in big-endian format back into an integer."""
    # Ensure that key_bytes is not None and is exactly 8 bytes
    if key_bytes is not None and len(key_bytes) == 8:
        return int.from_bytes(key_bytes, byteorder='big')
    else:
        # Handle cases where key_bytes is not 8 bytes as appropriate
        # This might include logging an error, raising an exception, or returning a default value
        return None  # Or your preferred way to handle this case

def is_ksqldb_available() -> bool:
    """
    Check if the ksqlDB server is available and running.

    :param ksql_url: The URL of the ksqlDB server (e.g., 'http://localhost:8088')
    :return: True if available, False otherwise
    """
    try:
        response = httpx.get(f"{get_ksqldb_url()}/info")
        if response.status_code == 200:
            info = response.json()
            if info.get('KsqlServerInfo', {}).get('serverStatus') == 'RUNNING':
                return True
        return False
    except Exception as e:
        logger.error(f"Error checking ksqlDB availability: {e}")
        return False

def get_ksqldb_url(kafka_ksqldb_endpoint_literal: KafkaKSqlDbEndPoint = KafkaKSqlDbEndPoint.KSQL) -> str:
    if utils.get_environment().upper() in ["DEV", None]:
        ksqldb_nodes: str = os.getenv("KSQLDB_STRING", "KSQLDB_NOT_DEFINED")
        if ksqldb_nodes == "KSQLDB_NOT_DEFINED" or ksqldb_nodes == "":
            ksqldb_nodes = ["http://localhost:8088"]
        return f"{random.choice(ksqldb_nodes)}/{kafka_ksqldb_endpoint_literal}"
    else:
        KSQLDB_STRING: str = os.getenv("KSQLDB_STRING", "KSQLDB_NOT_DEFINED")
        return f"{KSQLDB_STRING}/{kafka_ksqldb_endpoint_literal}"

def table_or_view_exists(name: str, connection_time_out: float = DEFAULT_CONNECTION_TIMEOUT) -> bool:
    """Checks, if the provided table or queryable already exists."""
    ksql_url = get_ksqldb_url(KafkaKSqlDbEndPoint.KSQL)
    response = httpx.post(ksql_url, json={"ksql": "LIST TABLES;"}, timeout=connection_time_out)
    logger.debug(f"Table Check Result: {response.status_code}: {response.text}")
    # Check if the request was successful
    if response.status_code == 200:
        tables = response.json()[0]["tables"]
        for table in tables:
            if str.lower(table["name"]) == str.lower(name):
                logger.debug(f"Table {name} exists.")
                return True
    elif "KSQL is not yet ready to serve requests." in response.text:
        logger.warning(f"KSQL is not ready to create the table {name}. Retrying...")
        raise KSQLNotReadyError("KSQL is not yet ready to serve requests.")
    else:
        logger.debug(f"Table {name} does not exists.")
        raise Exception(f'Failed to test if table or view exists in Kafka: {response.status_code}')

    return False

async def prepare_sql_statement(sql_statement: str) -> str:
    """If the DDL (sql_statement) one submits to create a table is a CTAS, then we:
    1) we parse the query for "KAFKA_TOPIC"
    2) check if the topic exists.
    3) if it exists, remove the PARTITIONS config of the sql_statement entirly to avoid conflicts.
    4) if it does not exists, we keep the sql_statement unmodified.
    """
    kafka_topic_match = re.search(r"KAFKA_TOPIC\s*=\s*'([^']+)'", sql_statement, re.IGNORECASE)
    if kafka_topic_match:
        partitions_match = re.search(r"PARTITIONS\s*=\s*\d+", sql_statement, re.IGNORECASE)
        kafka_topic = kafka_topic_match.group(1)
        if await topic_exists(kafka_topic):
            logger.info(f"Kafka topic {kafka_topic} exists.")
            if partitions_match:
                sql_statement = re.sub(r",?\s*PARTITIONS\s*=\s*\d+", "", sql_statement)
                logger.debug(f"PARTITIONS argument has been removed from the SQL statement, since the topic already exists. {sql_statement}")
            return sql_statement
        else:
            logger.info(f"Kafka topic {kafka_topic} does not exist. Setting PARTITIONS to 3 if not specified.")
            if not partitions_match:
                sql_statement = re.sub(r"\);", ", PARTITIONS=3);", sql_statement)
            return sql_statement
    return sql_statement

def clean_sql_statement(sql_statement: str) -> str:
    """Cleans the SQL statement by removing unnecessary spacing, newlines, and tabs."""
    return ' '.join(sql_statement.split())

async def create_table(sql_statement: str, table_name: str):
    """The invocation of this function will retry endlessly if the httpx.RemoteProtocolError or httpx.ConnectError occures. This implies, that the cluster is not yet ready and thus we need to retry.
    For all other exceptions, we retry for 60 seconds (every 5 seconds).    
    """
    sql_statement = clean_sql_statement(sql_statement)
    sql_statement = await prepare_sql_statement(sql_statement)

    headers = {"Content-Type": "application/json"}
    response = httpx.post(f"{get_ksqldb_url(KafkaKSqlDbEndPoint.KSQL)}", json={"ksql": sql_statement}, headers=headers, timeout=30)

    if response.status_code == 200:
        logger.info(f"Successfully created table {table_name}.")
    else:
        if "A table with the same name already exists" in response.text:
            logger.info(f"Table {table_name} already exists. Skipping creation.")
            return
        elif "KSQL is not yet ready to serve requests." in response.text:
            logger.warning(f"KSQL is not ready to create the table {table_name}. Retrying...")
            raise KSQLNotReadyError("KSQL is not yet ready to serve requests.")
        else:
            error_msg = f"Failed to create table {table_name}: {response.text}"
            logger.error(error_msg)
            raise Exception(error_msg)

    # Wait until the table is created
    max_wait_time = 60  # seconds
    poll_interval = 5  # seconds
    elapsed_time = 0

    while elapsed_time < max_wait_time:
        if table_or_view_exists(table_name):
            logger.info(f"Table {table_name} is now available.")
            return
        else:
            logger.debug(f"Table {table_name} is not yet available. Waiting...")
            await asyncio.sleep(poll_interval)
            elapsed_time += poll_interval

    logger.error(f"Timed out waiting for table {table_name} to be created.")
    raise TimeoutError(f"Timed out waiting for table {table_name} to be created.")

def stream_exists(name: str, connection_time_out: float = 60.0) -> bool:
    """Checks, if the provided table or queryable already exists."""
    ksql_url = get_ksqldb_url(KafkaKSqlDbEndPoint.KSQL)
    response = httpx.post(ksql_url, json={"ksql": "LIST STREAMS;"}, timeout=connection_time_out)
    logger.debug(f"Stream Check Result: {response}")
    print(f"{response.status_code}: {response.text}")
    # Check if the request was successful
    if response.status_code == 200:
        streams = response.json()[0]["streams"]
        for stream in streams:
            if str.lower(stream["name"]) == str.lower(name):
                logger.debug(f"Stream {name} exists.")
                return True
    elif "KSQL is not yet ready to serve requests." in response.text:
        logger.warning(f"KSQL is not ready to create the stream {name}. Retrying...")
        raise KSQLNotReadyError("KSQL is not yet ready to serve requests.")
    else:
        logger.debug(f"Stream {name} does not exists.")
        raise Exception(f'Failed to test if stream exists in Kafka: {response.status_code}')

    return False


async def create_stream(sql_statement: str, stream_name: str):
    """The invocation of this function will retry endlessly if the httpx.RemoteProtocolError or httpx.ConnectError occures. This implies, that the cluster is not yet ready and thus we need to retry.
    For all other exceptions, we retry for 60 seconds (every 5 seconds).    
    """
    sql_statement = clean_sql_statement(sql_statement)
    sql_statement = await prepare_sql_statement(sql_statement)

    headers = {"Content-Type": "application/json"}
    response = httpx.post(f"{get_ksqldb_url(KafkaKSqlDbEndPoint.KSQL)}", json={"ksql": sql_statement}, headers=headers, timeout=30)

    if response.status_code == 200:
        logger.info(f"Successfully created stream {stream_name}.")
    else:
        if "A stream with the same name already exists" in response.text:
            logger.info(f"Stream {stream_name} already exists. Skipping creation.")
            return
        elif "KSQL is not yet ready to serve requests." in response.text:
            logger.warning(f"KSQL is not ready to create the stream {stream_name}. Retrying...")
            raise KSQLNotReadyError("KSQL is not yet ready to serve requests.")
        else:
            error_msg = f"Failed to create stream {stream_name}: {response.text}"
            logger.error(error_msg)
            raise Exception(error_msg)

    # Wait until the table is created
    max_wait_time = 60  # seconds
    poll_interval = 5  # seconds
    elapsed_time = 0

    while elapsed_time < max_wait_time:
        if stream_exists(stream_name):
            logger.info(f"Stream {stream_name} is now available.")
            return
        else:
            logger.debug(f"Stream {stream_name} is not yet available. Waiting...")
            await asyncio.sleep(poll_interval)
            elapsed_time += poll_interval

    logger.error(f"Timed out waiting for stream {stream_name} to be created.")
    raise TimeoutError(f"Timed out waiting for stream {stream_name} to be created.")

async def execute_sql(sql: str, connection_time_out: float = DEFAULT_CONNECTION_TIMEOUT):
    """Executes the provided sql command. To create tables, use the create_table function instead."""

    ksql_url = get_ksqldb_url(KafkaKSqlDbEndPoint.KSQL)
    response = httpx.post(ksql_url, json={"ksql": sql}, timeout=connection_time_out)

    # Check if the request was successful
    if response.status_code == 200:
        logger.info(f"The provided SQL statement executed successfully. SQL: {sql}")
    else:
        raise Exception(f"Failed to execute SQL statement: {response.status_code}. SQL: {sql}")

async def produce_message(topic_name: str, key: str, value: any) -> None:
    """Will send the provided message to the specified Kafka topic and ends the producer when accomplished.."""
    kp = await get_default_kafka_producer()

    await kp.start()

    try:
        await kp.send_and_wait(topic=topic_name, key=key, value=value)
    except KafkaError as ke:
        error_message = f"""An error occurred when trying to send a message of type {type(object)} to the database. 
                        Error message: {ke}"""

        logger.error(error_message)
        raise Exception(error_message)
    except Exception as ex:
        error_message = f"""A general error occurred when trying to send a message of type {type(object)}
                        to the database. Error message: {ex}"""

        logger.error(error_message)
        raise Exception(error_message)
    finally:
        # Wait for all pending messages to be delivered or expire.
        await kp.flush()
        await kp.stop()
