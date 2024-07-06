import pytest
import asyncio
from aiokafka.errors import KafkaError
from aiokafka import AIOKafkaConsumer
from faessentials import database

DEFAULT_ENCODING = 'utf-8'
TEST_TOPIC = "test_topic"
CONSUMER_TIMEOUT = 10  # 10 seconds

@pytest.mark.asyncio
async def test_produce_message():
    # Arrange: Set up the test message and key
    test_key = "test_key"
    test_value = {"message": "test_value"}

    # Create a consumer to verify the produced message
    consumer: AIOKafkaConsumer = await database.get_default_kafka_consumer(
        TEST_TOPIC,
        auto_offset_reset='earliest',  # Set to read from the earliest offset
        auto_commit=False  # Disable auto commit for test isolation
    )

    # Produce the message
    await produce_message(TEST_TOPIC, test_key, test_value)

    # Act: Consume the message
    consumed_message = None
    try:
        async for msg in consumer:
            consumed_message = msg
            break
    except asyncio.TimeoutError:
        assert False, "Test timed out while waiting for message"
    finally:
        await consumer.stop()

    # Assert: Verify the consumed message matches the produced message
    print(f"Consumed Message: {consumed_message}")
    print(f"Key: {consumed_message.key}")
    print(f"Val: {consumed_message.value}")
    assert consumed_message is not None, "No message was consumed"
    assert consumed_message.key == test_key
    assert consumed_message.value == test_value

# Helper function to produce a message
async def produce_message(topic_name: str, key: str, value: any) -> None:
    kp = await database.get_default_kafka_producer()

    print("Having a producer. Starting to produce a message.")
    try:
        await kp.send_and_wait(topic=topic_name, key=key, value=value)
    except KafkaError as ke:
        error_message = f"""An error occurred when trying to send a message of type {type(object)} to the database. 
                        Error message: {ke}"""

        print(error_message)
        raise Exception(error_message)
    except Exception as ex:
        error_message = f"""A general error occurred when trying to send a message of type {type(object)}
                        to the database. Error message: {ex}"""

        print(error_message)
        raise Exception(error_message)
    finally:
        # Wait for all pending messages to be delivered or expire.
        await kp.flush()
        await kp.stop()

# Ensure you have a running Kafka instance and the required topic is created
if __name__ == "__main__":
    asyncio.run(asyncio.wait_for(test_produce_message(), timeout=CONSUMER_TIMEOUT))
