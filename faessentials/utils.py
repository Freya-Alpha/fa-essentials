import logging
import os
from pathlib import Path
import yaml
import sys
from redis.cluster import RedisCluster, ClusterNode

logger = logging.getLogger("app")

def get_project_root() -> str:
    str_path = str(Path(__file__).parent.parent)
    # print(f"utils.py: {str_path}")
    return str_path

def get_project_root_path() -> Path:
    abs_path = Path(__file__).parent.parent.absolute()
    return abs_path

def get_log_path() -> Path:
    abs_path = get_project_root_path().joinpath("logs")
    return abs_path

def get_secrets_path() -> Path:
    abs_path = get_project_root_path().joinpath("secrets")
    return abs_path

def get_app_config() -> dict:
    app_cfg = None
    with open(
        get_project_root_path().joinpath("config/app_config.yaml"), "r"
    ) as ymlfile:
        try:
            app_cfg = yaml.safe_load(ymlfile)
        except yaml.YAMLError as ex:
            logger.critical(
                f"Failed to load the app_config.yaml file. Aborting the application. Error: {ex}"
            )
            sys.exit(1)
    return app_cfg

def get_application_name() -> str:
    return get_app_config().get("application", "NO APPLICATION NAME")

def get_domain_name() -> str:
    return get_app_config().get("domain", "NO DOMAIN NAME")

def get_environment() -> str:
    return os.environ.get("ENV", get_app_config()["env"])

def get_logging_level() -> str:
    return get_app_config().get("logging_level", os.getenv("LOGGING_LEVEL", "DEBUG")).upper()

def get_redis_cluster_service_name():
    """Reads the node list from the environemnt variables.
    For all environements BUT the DEV environment.
    For PROD/UAT the Kubernetes Service will route the requests to any of the leaders,
    summarized by redis-cluster-leader
    """
    nodes_env = os.getenv("REDIS_CLUSTER_NODES", "redis-cluster-leader:6379")
    return nodes_env.split(":")


def get_redis_cluster_client() -> RedisCluster:
    """Creates a redis client to access the redis cluster in the current environment.
    That could be PROD, UAT or DEV."""
    rc: RedisCluster = None
    if get_environment().upper() == "DEV":
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
            startup_nodes=nodes,
            decode_responses=True,
            skip_full_coverage_check=True,
            address_remap=address_remap,
        )
    else:
        # PROD/UAT (any non-DEV environment)
        host_name, port = get_redis_cluster_service_name()
        logger.debug(f"host: {host_name}:{port}")
        if not host_name:
            raise Exception("No Redis cluster nodes in app_config file.")
            # TODO add the error log as soon this common code is in the library.
        rc = RedisCluster(
            host=host_name,
            port=int(port),
            decode_responses=True,
            # skip_full_coverage_check=True,
        )

    return rc
