from faessentials.database import get_kafka_cluster_brokers
from unittest.mock import patch


def test_get_kafka_cluster_brokers_dev():
    # Mock utils.get_environment() to return 'DEV'
    with patch('faessentials.utils.get_environment', return_value='DEV'):
        brokers = get_kafka_cluster_brokers()
        # Check if the returned brokers are as expected
        assert brokers == ['localhost:9092', 'localhost:9093', 'localhost:9094']
