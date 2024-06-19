import os
import pathlib
from pathlib import Path
import yaml

PROJECT_ROOT = None


# Determine the project root path when the module is loaded
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
    raise FileNotFoundError(f"Could not find the project root within the provided path {current_path} \
                            with the max depth of {max_depth}. \
                            The current path is {current_path}. \
                            Ensure the 'config' or 'logs' folder exists in {str(current_path)}. \
                            The PROJECT_ROOT environment variable is: {project_root_env}")


# Initialize PROJECT_ROOT when the module is loaded
def initialize_project_root():
    global PROJECT_ROOT
    PROJECT_ROOT = find_project_root(pathlib.Path(os.getcwd()).resolve())


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
        project_root = find_project_root(pathlib.Path(os.getcwd()).resolve())
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
