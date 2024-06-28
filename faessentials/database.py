import json
import os
import random
import socket
from enum import Enum
import time
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


async def get_default_kafka_producer() -> AIOKafkaProducer:
    """ This default producer is expecting you to send json data, which it will then automatically
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
        # if isinstance(k, str):
        #     return k.encode(DEFAULT_ENCODING)
        # else:
        #     return k.to_bytes(8, byteorder='big')

    producer: AIOKafkaProducer = AIOKafkaProducer(
        bootstrap_servers=broker_str,
        key_serializer=lambda k: get_key_serializer(k),
        value_serializer=lambda v: get_value_serializer(v))
    # start the producer for the client (it is often forgotten).
    await producer.start()
    return producer


async def get_default_kafka_consumer(topics: str, client: str = socket.gethostname(), consumer_group: str = None, auto_commit: bool = True) -> AIOKafkaConsumer:
    """ Will return an async-capable consumer.
        However, you may create your own consumer with specific settings. This is only for convenience.
        The offset could be set to 'earliest'. Default is 'latest'.
        : param auto_commit (True): Set auto_commit to False to control the commits yourself.
    """
    brokers: List[str] = get_kafka_cluster_brokers()
    broker_str = ",".join(brokers)
    # Create the Producer instance
    consumer: AIOKafkaConsumer = AIOKafkaConsumer(topics,
                                                  bootstrap_servers=broker_str,
                                                  client_id=client,
                                                  group_id=consumer_group,
                                                  key_deserializer=bytes_to_int_big_endian,
                                                  value_deserializer=lambda v: json.loads(v.decode(DEFAULT_ENCODING)),
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
    logger.debug(f"Table Check Result: {response}")
    print(f"{response.status_code}: {response.text}")
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

def create_table(sql_statement: str, table_name: str):
    """The invocation of this function will retry endlessly if the httpx.RemoteProtocolError or httpx.ConnectError occures. This implies, that the cluster is not yet ready and thus we need to retry.
    For all other exceptions, we retry for 60 seconds (every 5 seconds)."""
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
            time.sleep(poll_interval)
            elapsed_time += poll_interval

    logger.error(f"Timed out waiting for table {table_name} to be created.")
    raise TimeoutError(f"Timed out waiting for table {table_name} to be created.")

async def execute_sql(sql: str, connection_time_out: float = DEFAULT_CONNECTION_TIMEOUT):
    """Executes the provided sql command."""

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
