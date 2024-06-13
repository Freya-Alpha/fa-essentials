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
