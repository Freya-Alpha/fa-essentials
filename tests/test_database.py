import time

import pytest
from faessentials import database, utils
from faessentials.database import KSQLNotReadyError
from unittest.mock import patch
import httpx

@pytest.fixture(scope="module")
def setup():
    print("\n")
    print("REMINDER. All tests require a Kafka broker and a ksqlDB instance to be available.")
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

@pytest.mark.asyncio
async def test_create_table():
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
            await database.create_table(sql_statement, TABLE_NAME)
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

@pytest.mark.asyncio
async def test_prepare_sql_statement():
    sql_statement = """
    CREATE TABLE TEST_TABLE_PARTITION(
        event_timestamp BIGINT PRIMARY KEY,
        detail STRING,
        data STRING,
        signal_data STRUCT<
            provider_signal_id STRING,
            provider_trade_id STRING,
            provider_id STRING,
            strategy_id STRING,
            is_hot_signal BOOLEAN,
            market STRING,
            data_source STRING,
            direction STRING,
            side STRING,
            order_type STRING,
            price DOUBLE,
            tp DOUBLE,
            sl DOUBLE,
            position_size_in_percentage INT,
            date_of_creation BIGINT
        >,
        ip STRING
    ) WITH (
        KAFKA_TOPIC='test_topic_partition',
        VALUE_FORMAT='JSON',
        TIMESTAMP='event_timestamp',
        PARTITIONS=3
    );
    """

    with patch('faessentials.database.topic_exists', return_value=True):
        prepared_statement = await database.prepare_sql_statement(sql_statement)
        assert "PARTITIONS=3" not in prepared_statement

    with patch('faessentials.database.topic_exists', return_value=False):
        prepared_statement = await database.prepare_sql_statement(sql_statement)
        assert "PARTITIONS=3" in prepared_statement

@pytest.mark.asyncio
async def test_prepare_sql_statement_missing():
    """This test checks if the preparation works as well, wenn there is no PARTITION?"""
    sql_statement = """
    CREATE TABLE TEST_TABLE_PARTITION(
        event_timestamp BIGINT PRIMARY KEY,
        detail STRING,
        data STRING,
        signal_data STRUCT<
            provider_signal_id STRING,
            provider_trade_id STRING,
            provider_id STRING,
            strategy_id STRING,
            is_hot_signal BOOLEAN,
            market STRING,
            data_source STRING,
            direction STRING,
            side STRING,
            order_type STRING,
            price DOUBLE,
            tp DOUBLE,
            sl DOUBLE,
            position_size_in_percentage INT,
            date_of_creation BIGINT
        >,
        ip STRING
    ) WITH (
        KAFKA_TOPIC='test_topic_partition',
        VALUE_FORMAT='JSON',
        TIMESTAMP='event_timestamp'
    );
    """

    with patch('faessentials.database.topic_exists', return_value=True):
        prepared_statement = await database.prepare_sql_statement(sql_statement)
        assert "PARTITIONS=3" not in prepared_statement

    with patch('faessentials.database.topic_exists', return_value=False):
        prepared_statement = await database.prepare_sql_statement(sql_statement)
        assert "PARTITIONS=3" in prepared_statement

def test_clean_sql_statement():
    sql_statement = """
    CREATE TABLE fa_signal_processing_trading_signal_received(
        event_timestamp BIGINT PRIMARY KEY,
        detail STRING,
        data STRING,
        signal_data STRUCT<
            provider_signal_id STRING,
            provider_trade_id STRING,
            provider_id STRING,
            strategy_id STRING,
            is_hot_signal BOOLEAN,
            market STRING,
            data_source STRING,
            direction STRING,
            side STRING,
            order_type STRING,
            price DOUBLE,
            tp DOUBLE,
            sl DOUBLE,
            position_size_in_percentage INT,
            date_of_creation BIGINT
        >,
        ip STRING
    ) WITH (
        KAFKA_TOPIC='fa_signal_processing.trading_signal_received',
        VALUE_FORMAT='JSON',
        TIMESTAMP='event_timestamp',
        PARTITIONS=3
    );
    """

    expected_cleaned_sql = "CREATE TABLE fa_signal_processing_trading_signal_received( event_timestamp BIGINT PRIMARY KEY, detail STRING, data STRING, signal_data STRUCT< provider_signal_id STRING, provider_trade_id STRING, provider_id STRING, strategy_id STRING, is_hot_signal BOOLEAN, market STRING, data_source STRING, direction STRING, side STRING, order_type STRING, price DOUBLE, tp DOUBLE, sl DOUBLE, position_size_in_percentage INT, date_of_creation BIGINT >, ip STRING ) WITH ( KAFKA_TOPIC='fa_signal_processing.trading_signal_received', VALUE_FORMAT='JSON', TIMESTAMP='event_timestamp', PARTITIONS=3 );"

    cleaned_sql = database.clean_sql_statement(sql_statement)
    assert cleaned_sql == expected_cleaned_sql

def test_stream_exists():
    max_retries = 5
    delay = 10  # seconds

    for attempt in range(max_retries):
        try:
            assert not database.stream_exists("NON_EXISTENT_STREAM")
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

@pytest.mark.asyncio
async def test_create_stream():
    STREAM_NAME = "TEST_STREAM"
    sql_statement = f"""
    CREATE STREAM {STREAM_NAME}(
        event_timestamp BIGINT,
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
            await database.create_stream(sql_statement, STREAM_NAME)
            assert database.stream_exists(STREAM_NAME)
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
