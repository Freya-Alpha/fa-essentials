import os
import pathlib
import pytest
import yaml
from faessentials import utils

@pytest.fixture
def mock_project_root(monkeypatch, tmp_path):
    # Directly set the PROJECT_ROOT in the utils module
    utils.PROJECT_ROOT = tmp_path
    return tmp_path

def test_get_project_root(mock_project_root):
    expected_path = str(mock_project_root)
    assert utils.get_project_root() == expected_path


def test_get_project_root_path(mock_project_root):
    assert utils.get_project_root_path() == mock_project_root


def test_get_app_config(monkeypatch, tmp_path):
    # Create a temporary config directory
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    mock_config_file = config_dir / "app_config.yaml"

    # Write the mock configuration
    expected_config = {
        "application": "TestApp",
        "domain": "test.domain.com",
        "env": "DEV",
        "logging_level": "INFO"
    }
    with open(mock_config_file, "w") as file:
        yaml.dump(expected_config, file)

    # Override the `find_project_root` function to return the tmp_path
    def mock_find_project_root(*args, **kwargs):
        return tmp_path

    monkeypatch.setattr(utils, "find_project_root", mock_find_project_root)

    # Run the test
    assert utils.get_app_config() == expected_config

def test_find_project_root():
    try:        
        project_root = utils.find_project_root(pathlib.Path(__file__).resolve())
        print(project_root)
    except Exception as e:
        # If any exception is caught, fail the test explicitly.
        assert False, f"An exception was thrown: {e}"

def test_get_application_name_success(monkeypatch):
    def mock_get_app_config():
        return {"application": "TestApp"}

    monkeypatch.setattr(utils, "get_app_config", mock_get_app_config)
    assert utils.get_application_name() == "TestApp"

def test_get_application_name_failure(monkeypatch):
    def mock_get_app_config():
        return {}

    monkeypatch.setattr(utils, "get_app_config", mock_get_app_config)
    with pytest.raises(ValueError):
        utils.get_application_name()

def test_get_domain_name_success(monkeypatch):
    def mock_get_app_config():
        return {"domain": "test.domain.com"}

    monkeypatch.setattr(utils, "get_app_config", mock_get_app_config)
    assert utils.get_domain_name() == "test.domain.com"

def test_get_domain_name_failure(monkeypatch):
    def mock_get_app_config():
        return {}

    monkeypatch.setattr(utils, "get_app_config", mock_get_app_config)
    with pytest.raises(ValueError):
        utils.get_domain_name()

def test_get_redis_cluster_service_name_with_env_in_dev(monkeypatch):
    monkeypatch.setenv("REDIS_CLUSTER_NODES", "redis-node1:1234")
    monkeypatch.setenv("ENV", "DEV")
    expected_result = ["UNDEFINED - EMPLOYING LOCAL CLUSTER"]
    assert utils.get_redis_cluster_service_name() == expected_result

def test_get_redis_cluster_service_name_with_env_in_prod(monkeypatch):
    monkeypatch.setenv("REDIS_CLUSTER_NODES", "redis-node1:1234")
    monkeypatch.setenv("ENV", "PROD")
    expected_result = ["redis-node1", "1234"]
    assert utils.get_redis_cluster_service_name() == expected_result

def test_get_redis_cluster_service_name_default(monkeypatch):
    monkeypatch.delenv("REDIS_CLUSTER_NODES", raising=False)
    expected_result = ["UNDEFINED - EMPLOYING LOCAL CLUSTER"]
    assert utils.get_redis_cluster_service_name() == expected_result

@pytest.mark.skipif(os.environ.get("GITHUB_ACTIONS") == "true", reason="Requires DEV environment with a local Redis cluster and should not run on GitHub Actions")
def test_get_redis_cluster_client_add_method():
    # Obtain the RedisCluster client
    rc = utils.get_redis_cluster_client()

    # Perform a test operation - add a key with a value
    test_key = "test_key"
    test_value = "test_value"
    rc.set(test_key, test_value)

    # Retrieve the value to verify the operation
    value = rc.get(test_key)

    # Clean up by deleting the test key
    rc.delete(test_key)

    # Assert that the retrieved value matches the test value
    assert value == test_value
