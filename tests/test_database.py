import time

import pytest
from faessentials import database, utils
from faessentials.database import KSQLNotReadyError
from unittest.mock import patch
import httpx

@pytest.fixture(scope="module")
def setup():
    print("\n")
    print("All tests require a Kafka broker and a ksqlDB instance to be available.")
    print(f"\tENV: {utils.get_environment()}")
    print(f"\tkafka: {database.get_kafka_cluster_brokers()}")
    print(f"\tksqldb: {database.get_ksqldb_url()}")

def test_get_kafka_cluster_brokers_dev():
    # Mock utils.get_environment() to return 'DEV'
    with patch('faessentials.utils.get_environment', return_value='DEV'):
        brokers = database.get_kafka_cluster_brokers()
        # Check if the returned brokers are as expected
        assert brokers == ['localhost:9092']

def test_get_kafka_cluster_brokers_non_dev():
    # Mock utils.get_environment() to return 'PROD'
    with patch('faessentials.utils.get_environment', return_value='PROD'):
        # Mock os.getenv to return a specific broker string
        with patch('os.getenv', return_value='broker1.svc.cluster.local:9092,broker2.svc.cluster.local:9093'):
            brokers = database.get_kafka_cluster_brokers()
            # Check if the returned brokers contain the expected substring
            assert 'broker1.svc.cluster.local:9092' in brokers
            assert brokers == ['broker1.svc.cluster.local:9092', 'broker2.svc.cluster.local:9093']

def test_table_or_view_exists():
    max_retries = 5
    delay = 10  # seconds

    for attempt in range(max_retries):
        try:
            assert not database.table_or_view_exists("NON_EXISTENT_TABLE")
            break
        except (
            httpx.ConnectError,
            KSQLNotReadyError,
            httpx.RemoteProtocolError,
            httpx.ReadError,
        ) as e:
            if attempt < max_retries - 1:
                print(
                    f"Attempt {attempt + 1}/{max_retries} failed with error: {e}. Retrying in {delay} seconds..."
                )
                time.sleep(delay)
            else:
                raise
        except Exception as e:
            print(f"Test failed with an unexpected error: {e}. {e.args}")
            raise

def test_create_table():
    TABLE_NAME = "TEST_TABLE"
    sql_statement = f"""
    CREATE TABLE {TABLE_NAME}(
        event_timestamp BIGINT PRIMARY KEY,
        detail STRING,
        data STRING
    ) WITH (
        KAFKA_TOPIC='test_topic',
        VALUE_FORMAT='JSON',
        PARTITIONS=1
    );
    """

    max_retries = 5
    delay = 10  # seconds

    for attempt in range(max_retries):
        try:
            database.create_table(sql_statement, TABLE_NAME)
            assert database.table_or_view_exists(TABLE_NAME)
            break
        except (
            httpx.ConnectError,
            KSQLNotReadyError,
            httpx.RemoteProtocolError,
            httpx.ReadError,
        ) as e:
            if attempt < max_retries - 1:
                print(
                    f"Attempt {attempt + 1}/{max_retries} failed with error: {e}. Retrying in {delay} seconds..."
                )
                time.sleep(delay)
            else:
                raise
        except Exception as e:
            print(f"Test failed with an unexpected error: {e}")
            raise