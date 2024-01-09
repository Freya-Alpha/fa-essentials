import os
import pathlib
from pathlib import Path
import yaml
from redis.cluster import RedisCluster, ClusterNode

# Determine the project root path when the module is loaded
PROJECT_ROOT = None

def find_project_root(current_path: pathlib.Path, max_depth: int = 10) -> pathlib.Path:
    """
    Recursively search for a marker (like the 'config' or 'logs' directory) to find the project root.
    """

    # Check if PROJECT_ROOT environment variable is set
    project_root_env = os.getenv('PROJECT_ROOT')
    if project_root_env:
        return pathlib.Path(project_root_env)

    for _ in range(max_depth):
        if (current_path / "config").exists() or (current_path / "logs").exists():
            return current_path
        current_path = current_path.parent
    raise FileNotFoundError(f"Could not find the project root. Ensure the 'config' or 'logs' folder exists in {str(current_path)}. The PROJECT_ROOT environement variable is: {project_root_env}")


# Initialize PROJECT_ROOT when the module is loaded
def initialize_project_root():
    global PROJECT_ROOT
    PROJECT_ROOT = find_project_root(pathlib.Path(__file__).resolve())

initialize_project_root()

def get_project_root_path() -> Path:
    """
    Return the project root path.
    """
    return PROJECT_ROOT

def get_project_root() -> str:
    str_path = str(PROJECT_ROOT)
    # print(f"utils.py: {str_path}")
    return str_path

def get_log_path() -> Path:
    abs_path = get_project_root_path().joinpath("logs")
    return abs_path

def get_secrets_path() -> Path:
    abs_path = get_project_root_path().joinpath("secrets")
    return abs_path

def get_app_config() -> dict:
    app_cfg = None
    try:
        project_root = find_project_root(pathlib.Path(__file__).resolve())
        config_path = project_root.joinpath("config/app_config.yaml")
        with open(config_path, "r") as ymlfile:
            app_cfg = yaml.safe_load(ymlfile)
    except yaml.YAMLError as ex:
        raise FileNotFoundError(
            f"Failed to load the config/app_config.yaml file. Aborting the application. Error: {ex}"
        )
    return app_cfg

def get_application_name() -> str:
    app_name = get_app_config().get("application")
    if app_name is None:
        raise ValueError("Application name not found in app_config.")
    return app_name

def get_domain_name() -> str:
    domain_name = get_app_config().get("domain")
    if domain_name is None:
        raise ValueError("Domain name not found in app_config.")
    return domain_name

def get_environment() -> str:
    """Will fetch the environment variable ENV. If not present it will fall back to DEV """
    return os.environ.get("ENV", "DEV")

def get_service_url() -> str:
    """This own service url value. This global environment variable is usually used by consumers apps of this API."""
    return os.getenv("OPENAPI_SERVICE_URL", "http://localhost:8080")

def get_service_doc_url() -> str:
    """Return the OpenAPI url"""
    return f"{get_service_url}/docs"


def get_logging_level() -> str:
    return get_app_config().get("logging_level", os.getenv("LOGGING_LEVEL", "DEBUG")).upper()

def get_redis_cluster_service_name():
    """Reads one service name and one port from the environemnt variable.
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
