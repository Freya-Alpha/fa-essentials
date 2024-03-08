import pytest
from unittest.mock import patch, MagicMock
import redis
from faessentials.database import (ensure_consumer_group)

@pytest.fixture
def mock_redis_client():
    return MagicMock()

@patch('faessentials.database.get_redis_cluster_client')
def test_create_group_success(mock_get_redis_cluster_client, mock_redis_client):
    # Mocking Redis client
    mock_get_redis_cluster_client.return_value = mock_redis_client

    # Call the function with mock Redis client
    ensure_consumer_group('stream_key', 'group_name')

    # Assert that xgroup_create method is called with expected arguments
    mock_redis_client.xgroup_create.assert_called_once_with(
        name='stream_key', groupname='group_name', id='$', mkstream=True
    )

@patch('faessentials.database.get_redis_cluster_client')
def test_connection_error(mock_get_redis_cluster_client, mock_redis_client):
    # Mocking Redis client
    mock_get_redis_cluster_client.return_value = mock_redis_client

    # Mocking xgroup_create method to raise ConnectionError
    mock_redis_client.xgroup_create.side_effect = redis.exceptions.ConnectionError()

    # Call the function and assert that ConnectionError is raised
    with pytest.raises(ConnectionError):
        ensure_consumer_group(stream_key='stream_key', group_name='group_name')
