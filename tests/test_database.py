from faessentials.database import get_kafka_cluster_brokers
from unittest.mock import patch


def test_get_kafka_cluster_brokers_dev():
    # Mock utils.get_environment() to return 'DEV'
    with patch('faessentials.utils.get_environment', return_value='DEV'):
        brokers = get_kafka_cluster_brokers()
        # Check if the returned brokers are as expected
        assert brokers == ['localhost:9092', 'localhost:9093', 'localhost:9094']

def test_get_kafka_cluster_brokers_non_dev():
    # Mock utils.get_environment() to return 'PROD'
    with patch('faessentials.utils.get_environment', return_value='PROD'):
        # Mock os.getenv to return a specific broker string
        with patch('os.getenv', return_value='broker1.svc.cluster.local:9092,broker2.svc.cluster.local:9093'):
            brokers = get_kafka_cluster_brokers()
            # Check if the returned brokers contain the expected substring
            assert 'broker1.svc.cluster.local:9092' in brokers
            assert brokers == ['broker1.svc.cluster.local:9092', 'broker2.svc.cluster.local:9093']