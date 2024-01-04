from pathlib import Path
import pytest
import yaml
from faessentials import utils

# Mock for the __file__ attribute in utils module
utils.__file__ = __file__

def test_get_project_root():
    expected_path = str(Path(__file__).parent.parent)
    assert utils.get_project_root() == expected_path

def test_get_project_root_path():
    expected_path = Path(__file__).parent.parent.absolute()
    assert utils.get_project_root_path() == expected_path

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

    # Mock get_project_root_path to return the temporary directory
    def mock_get_project_root_path():
        return tmp_path

    monkeypatch.setattr(utils, "get_project_root_path", mock_get_project_root_path)

    # Run the test
    assert utils.get_app_config() == expected_config

def test_get_application_name_success(monkeypatch):
    # Mock get_app_config to return a dict with an application name
    def mock_get_app_config():
        return {"application": "TestApp"}

    monkeypatch.setattr(utils, "get_app_config", mock_get_app_config)
    assert utils.get_application_name() == "TestApp"

def test_get_application_name_failure(monkeypatch):
    # Mock get_app_config to return a dict without an application name
    def mock_get_app_config():
        return {}

    monkeypatch.setattr(utils, "get_app_config", mock_get_app_config)
    with pytest.raises(ValueError):
        utils.get_application_name()

def test_get_domain_name_success(monkeypatch):
    # Mock get_app_config to return a dict with a domain name
    def mock_get_app_config():
        return {"domain": "test.domain.com"}

    monkeypatch.setattr(utils, "get_app_config", mock_get_app_config)
    assert utils.get_domain_name() == "test.domain.com"

def test_get_domain_name_failure(monkeypatch):
    # Mock get_app_config to return a dict without a domain name
    def mock_get_app_config():
        return {}

    monkeypatch.setattr(utils, "get_app_config", mock_get_app_config)
    with pytest.raises(ValueError):
        utils.get_domain_name()

def test_get_redis_cluster_service_name_with_env(monkeypatch):
    monkeypatch.setenv("REDIS_CLUSTER_NODES", "redis-node1:1234")

    expected_result = ["redis-node1", "1234"]
    assert utils.get_redis_cluster_service_name() == expected_result

def test_get_redis_cluster_service_name_default(monkeypatch):
    monkeypatch.delenv("REDIS_CLUSTER_NODES", raising=False)

    expected_result = ["redis-cluster-leader", "6379"]
    assert utils.get_redis_cluster_service_name() == expected_result
