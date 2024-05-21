import json
import os
import random
import socket
from typing import List
from redis import ResponseError
import redis
from redis.cluster import RedisCluster, ClusterNode
from tksessentials import utils
from kafka import KafkaProducer
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer


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
            raise ValueError("There is NO password for the Redis Cluster available with this deployment. Please see to it.")

    return rc

class GroupCreationResponseError(ResponseError):
    def __init__(self, message):
        self.message = message
        super().__init__(message)

def ensure_consumer_group(stream_key, group_name, start_reading_pos='$'):
    """Ensure the consumer group is created and allows the group to start reading from the current position. Set start_reading_pos to 0 to start consuming form the very first entry."""
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
    """Fetch the kafka broker array. This should return an array with nodes and ports. e.g. ['localhost:9092', 'localhost:9093']"""
    if utils.get_environment().upper() in ["DEV", None]:
        brokers = 'localhost:9092,localhost:9093,localhost:9094'
    else:
        brokers = os.getenv("KAFKA_CLUSTER_BROKERS", "NODES_NOT_DEFINED")
    return brokers.split(",")

def get_kafka_producer(client: str = socket.gethostname()) -> AIOKafkaProducer:
    """ This default producer is expecting you to send json data, which it will then automatically serialize/encode with UTF-8.
        The key must be a posix timestamp (int in python, BIGINT in Kafka).
        Feel free to create your own kafka producer, if these default values do no suite the use case.
    """
    brokers: List[str] = get_kafka_cluster_brokers()
    broker_str = ",".join(brokers)
    producer: AIOKafkaProducer = AIOKafkaProducer(bootstrap_servers=broker_str,
                                                  key_serializer=lambda k: k.to_bytes(8, byteorder='big'),
                                                  value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                                                  )
    return producer

def get_ksqldb_url() -> str:
    if utils.get_environment().upper() in ["DEV", None]:
        ksqldb_nodes = ["localhost:8088"]
        return f"http://{random.choice(ksqldb_nodes)}"
    else:
        # TODO: THIS SHOULD BE FETCHED FROM THE GLOBAL CONFIG-MAP FOR FA.
        return "http://ksqldb.sahri.local"

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
                                                  value_deserializer=lambda v: json.loads(v.decode('utf-8')))
    return consumer

def bytes_to_int_big_endian(key_bytes: bytes) -> int:
    """Converts 8 bytes in big-endian format back into an integer."""
    # Ensure that key_bytes is not None and is exactly 8 bytes
    if key_bytes is not None and len(key_bytes) == 8:
        return int.from_bytes(key_bytes, byteorder='big')
    else:
        # Handle cases where key_bytes is not 8 bytes as appropriate
        # This might include logging an error, raising an exception, or returning a default value
        return None  # Or your preferred way to handle this case