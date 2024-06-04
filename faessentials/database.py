import json
import os
import random
import socket
from datetime import datetime
from enum import Enum
from typing import List
import httpx
import pydantic
from aiokafka.errors import KafkaError
from redis import ResponseError
import redis
from redis.cluster import RedisCluster, ClusterNode
from faessentials import utils, global_logger
from faessentials.constants import DEFAULT_ENCODING, DEFAULT_CONNECTION_TIMEOUT
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer


class KafkaKSqlDbEndPoint(str, Enum):
    KSQL = "ksql"
    KSQL_TERMINATE = "ksql/terminate"
    QUERY = "query"
    QUERY_STREAM = "query-stream"
    STATUS = "status"
    INFO = "info"
    CLUSTER_STATUS = "clusterStatus"
    IS_VALID_PROPERTY = "is_valid_property"


def get_redis_cluster_service_name():
    """Fetch the redis cluster service: FQDN and port. """
    if utils.get_environment().upper() == "DEV" or utils.get_environment().upper() is None:
        nodes_env = "UNDEFINED - EMPLOYING LOCAL REDIS CLUSTER"
    else:
        nodes_env = os.getenv("REDIS_CLUSTER_NODES", "NODES_NOT_DEFINED")
    return nodes_env.split(":")


def get_redis_cluster_pw():
    return os.getenv("REDIS_CLUSTER_PW")


def get_redis_cluster_client() -> RedisCluster:
    """Creates a redis client to access the redis cluster in the current environment.
    That could be PROD, UAT or DEV."""
    rc: RedisCluster = None
    if utils.get_environment().upper() == "DEV":
        REDIS_SERVICE = os.environ.get("REDIS_SERVICE", "127.0.0.1")
        REDIS_PORTS = os.environ.get("REDIS_PORTS", "7000,7001,7002").split(",")
        nodes = [ClusterNode(REDIS_SERVICE, int(port)) for port in REDIS_PORTS]
        address_remap_dict = {
            "172.30.0.11:6379": ("127.0.0.1", 7000),
            "172.30.0.12:6379": ("127.0.0.1", 7001),
            "172.30.0.13:6379": ("127.0.0.1", 7002),
        }

        def address_remap(address):
            host, port = address
            return address_remap_dict.get(f"{host}:{port}", address)

        # rc = RedisCluster(startup_nodes=nodes, decode_responses=True, skip_full_coverage_check=True)
        rc = RedisCluster(
            username='default',
            password='my-password',
            startup_nodes=nodes,
            decode_responses=True,
            skip_full_coverage_check=True,
            address_remap=address_remap,
        )
    else:
        # PROD/UAT (any non-DEV environment)
        host_name, port = get_redis_cluster_service_name()
        if not host_name:
            raise Exception("No Redis cluster nodes in app_config file.")
            # TODO add the error log as soon this common code is in the library.

        PW = get_redis_cluster_pw()
        if isinstance(PW, str):
            rc = RedisCluster(
                host=host_name,
                port=int(port),
                username='default',
                password=PW,
                decode_responses=True,
                require_full_coverage=False,
                read_from_replicas=True
            )
        else:
            raise ValueError("There is NO password for the Redis Cluster available with this deployment. "
                             "Please see to it.")

    return rc


class GroupCreationResponseError(ResponseError):
    def __init__(self, message):
        self.message = message
        super().__init__(message)


def ensure_consumer_group(stream_key, group_name, start_reading_pos='$'):
    """Ensure the consumer group is created and allows the group to start reading from the current position.
    Set start_reading_pos to 0 to start consuming form the very first entry."""
    rc = get_redis_cluster_client()
    try:
        rc.xgroup_create(
            name=stream_key, groupname=group_name, id=start_reading_pos, mkstream=True
        )
    except ResponseError as e:
        if "BUSYGROUP Consumer Group name already exists" not in str(e):
            raise GroupCreationResponseError(
                f"Failed to create consumer group for stream {stream_key} and group {group_name}. {e}"
            )
    except (redis.exceptions.ConnectionError, ConnectionRefusedError) as con_error:
        raise ConnectionError(
            "Connection Error: Failed to connect to Redis."
        ) from con_error


def get_kafka_cluster_brokers() -> List[str]:
    """Fetch the kafka broker array. This should return an array with nodes and ports.
    e.g. ['localhost:9092', 'localhost:9093']"""
    if utils.get_environment().upper() in ["DEV", None]:
        brokers = 'localhost:9092,localhost:9093,localhost:9094'
    else:
        brokers = os.getenv("KAFKA_CLUSTER_BROKERS", "NODES_NOT_DEFINED")
    return brokers.split(",")


def get_kafka_producer() -> AIOKafkaProducer:
    """ This default producer is expecting you to send json data, which it will then automatically serialize/encode with UTF-8.
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
    return producer


def get_ksqldb_url(kafka_ksqldb_endpoint: KafkaKSqlDbEndPoint = KafkaKSqlDbEndPoint.KSQL) -> str:
    if utils.get_environment().upper() in ["DEV", None]:
        ksqldb_nodes = ["localhost:8088"]
        return f"http://{random.choice(ksqldb_nodes)}/{kafka_ksqldb_endpoint}"
    else:
        # TODO: THIS SHOULD BE FETCHED FROM THE GLOBAL CONFIG-MAP FOR FA.
        return f"http://ksqldb.sahri.local/{kafka_ksqldb_endpoint}"


def get_kafka_consumer(topics: str, client: str = socket.gethostname(),
                       consumer_group: str = None,
                       offset: str = 'latest'
                       ) -> AIOKafkaConsumer:
    """ Will return a async-capable consumer.
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


async def kafka_table_or_view_exists(name: str, connection_time_out: float = DEFAULT_CONNECTION_TIMEOUT) -> bool:
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
        raise Exception(f'Failed to test if table or view exist in Kafka: {response.status_code}')

    return False


async def kafka_execute_sql(sql: str, connection_time_out: float = DEFAULT_CONNECTION_TIMEOUT):
    """Executes the provided sql command."""
    logger = global_logger.setup_custom_logger("app")

    ksql_url = get_ksqldb_url(KafkaKSqlDbEndPoint.KSQL)
    response = httpx.post(ksql_url, json={"ksql": sql}, timeout=connection_time_out)

    # Check if the request was successful
    if response.status_code == 200:
        logger.info(f"The provided SQL statement executed successfully. SQL: {sql}")
    else:
        raise Exception(f"Failed to execute SQL statement: {response.status_code}. SQL: {sql}")


async def kafka_send_message(topic_name: str,
                             value: any,
                             key: str or int = int(round(datetime.now().timestamp()))) -> None:
    """Will send the provided message to the specified Kafka topic."""
    logger = global_logger.setup_custom_logger("app")
    kc = get_kafka_producer()

    await kc.start()

    try:
        await kc.send_and_wait(topic=topic_name, key=key, value=value)
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
        await kc.stop()
