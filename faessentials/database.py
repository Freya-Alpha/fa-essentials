import json
import os
import random
import socket
from enum import Enum
from typing import List
import httpx
import pydantic
from aiokafka.errors import KafkaError
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from faessentials import utils, global_logger
from faessentials.constants import DEFAULT_ENCODING, DEFAULT_CONNECTION_TIMEOUT


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
        brokers = 'localhost:9092,localhost:9093,localhost:9094'
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

    def get_key_serializer(k: int or str) -> bytes:
        if isinstance(k, str):
            return k.encode(DEFAULT_ENCODING)
        else:
            return k.to_bytes(8, byteorder='big')

    producer: AIOKafkaProducer = AIOKafkaProducer(
        bootstrap_servers=broker_str,
        key_serializer=lambda k: get_key_serializer(k),
        value_serializer=lambda v: get_value_serializer(v))
    # start the producer for the client (it is often forgotten).
    await producer.start()
    return producer


async def get_default_kafka_consumer(topics: str, client: str = socket.gethostname(), consumer_group: str = None) -> AIOKafkaConsumer:
    """ Will return an async-capable consumer.
        However, you may create your own consumer with specific settings. This is only for convenience.
        The offset could be set to 'earliest'. Default is 'latest'.
    """
    brokers: List[str] = get_kafka_cluster_brokers()
    broker_str = ",".join(brokers)
    # Create the Producer instance
    consumer: AIOKafkaConsumer = AIOKafkaConsumer(topics,
                                                  bootstrap_servers=broker_str,
                                                  client_id=client,
                                                  group_id=consumer_group,
                                                  key_deserializer=bytes_to_int_big_endian,
                                                  value_deserializer=lambda v: json.loads(v.decode(DEFAULT_ENCODING)))
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
        ksqldb_nodes = ["localhost:8088"]
        return f"http://{random.choice(ksqldb_nodes)}/{kafka_ksqldb_endpoint_literal}"
    else:
        # TODO: THIS SHOULD BE FETCHED FROM THE GLOBAL CONFIG-MAP FOR FA.
        #return f"http://ksqldb.sahri.local/{kafka_ksqldb_endpoint}"
        KSQLDB_STRING: str = os.getenv("KSQLDB_STRING", "NODES_NOT_DEFINED")
        return f"{KSQLDB_STRING}/{kafka_ksqldb_endpoint_literal}"


async def table_or_view_exists(name: str, connection_time_out: float = DEFAULT_CONNECTION_TIMEOUT) -> bool:
    """Checks, if the provided table or queryable already exists."""
    ksql_url = get_ksqldb_url(KafkaKSqlDbEndPoint.KSQL)
    response = httpx.post(ksql_url, json={"ksql": "LIST TABLES;"}, timeout=connection_time_out)

    # Check if the request was successful
    if response.status_code == 200:
        tables = response.json()[0]["tables"]
        for table in tables:
            if str.lower(table["name"]) == name:
                return True
    else:
        raise Exception(f'Failed to test if table or view exists in Kafka: {response.status_code}')

    return False


async def execute_sql(sql: str, connection_time_out: float = DEFAULT_CONNECTION_TIMEOUT):
    """Executes the provided sql command."""
    logger = global_logger.setup_custom_logger("app")

    ksql_url = get_ksqldb_url(KafkaKSqlDbEndPoint.KSQL)
    response = httpx.post(ksql_url, json={"ksql": sql}, timeout=connection_time_out)

    # Check if the request was successful
    if response.status_code == 200:
        logger.info(f"The provided SQL statement executed successfully. SQL: {sql}")
    else:
        raise Exception(f"Failed to execute SQL statement: {response.status_code}. SQL: {sql}")


async def produce_message(topic_name: str, key: str, value: any) -> None:
    """Will send the provided message to the specified Kafka topic."""
    logger = global_logger.setup_custom_logger("app")
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
        await kp.stop()
