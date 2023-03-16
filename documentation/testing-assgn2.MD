# TESTING 

## Test Files and Tests Conducted
We started with a single producer and consumer

This was divided into 2 categories:
- Testing without a fixed partition (round robin test) `single_prod_single_con_multi_partition.py`

  - **Test the endpoint create Topic**: To test if a new topic can be created on the server.

  - **Test the endpoint create Topic (duplicate topic)**: To test if the server responds correctly when a duplicate topic is created.

  - **Test the endpoint get topics**: To test if the server responds correctly when a request to get all topics is made.

  - **Test the endpoint register consumer**: To test if the server responds correctly when a consumer is registered for an existing topic.

  - **Test the endpoint register consumer (invalid topic name)**: To test if the server responds correctly when a consumer is registered for an invalid topic name.

  - **Test the endpoint register producer (existing topic)**: To test if the server responds correctly when a producer is registered for an existing topic.

  - **Test the endpoint register producer (new topic)**: To test if the server responds correctly when a producer is registered for a new topic.

  - **Calling the endpoint get Topics**: To test if the server responds correctly when a request to get all topics is made.

  - **Pushing messages on a Topic**: To test if messages can be pushed on a topic on the server.

- Testing with a fixed partition `single_prod_single_con_single_partition.py`

  - **Endpoint register producer (new topic)**: This test is responsible for registering a new producer for a new topic.

  - **Production to general partition (randomized)**: This test is responsible for producing random messages to a general partition of the topic.

  - **List partitions for a topic**: This test is responsible for listing all partitions of a given topic.

  - **Production to specific partition**: This test is responsible for producing random messages to a specific partition of the topic.

  - **Endpoint register consumer (topic level)**: This test is responsible for registering a new consumer at the topic level.

  - **Consuming messages using the consumer and reporting size for 2*num_of_messages times**: This test is responsible for consuming messages using the registered consumer at the topic level, and reporting the size of the topic after each consume request.

  - **Endpoint register consumer (partition level)**: This test is responsible for registering a new consumer at the partition level.

  - **Consuming messages using the consumer from partition and reporting size**: This test is responsible for consuming messages using the registered consumer at the partition level, and reporting the size of the partition after each consume request.


Then we checked if concurrent requests are being handled. We created 3 consumers and 5 producers and simultaneously produced and consumed messages from the queue. This was identical to the implementation done for Assignment 1. The Newly designed Messaging system worked as expected during testing

Finally we tested fault tolerance on 2 levels:
- Broker `broker_down.py` (NOTE : This file also tests the multi partition production)

  - **Register producer** : To test the functionality of the `/producer/register` endpoint by registering a new producer for the topic `topic_1`.

  - **Register consumer** : To test the functionality of the `/consumer/register` endpoint by registering a new consumer for the topic `topic_1`.

  - **Produce messages to general partition** : To test the functionality of the `/producer/produce` endpoint by producing 10 messages to the topic `topic_1`

  - **Consume messages and report size** : To test the functionality of the `/size` endpoint by reporting the size of the topic `topic_1` for the given consumer and to test the functionality of the `/consumer/consume` endpoint by consuming messages from the topic `topic_1` using the registered consumer.

  - **Produce messages to general partition after broker 1 down** : To test the behavior of the system when one broker is down, by producing 10 messages to the topic `topic_1` after making broker 1 down.

  - **Consume messages and report size after broker 1 down** : To test the behavior of the system when one broker is down, by consuming messages from the topic `topic_1` using the registered consumer and reporting the size of the topic for the given consumer.

  - **Produce messages to general partition after broker 2 down** : To test the behavior of the system when one broker is down, by producing 10 messages to the topic `topic_1` after making broker 2 down.

  - **Consume messages and report size after broker 2 down** : To test the behavior of the system when one broker is down, by consuming messages from the topic `topic_1` using the registered consumer and reporting the size of the topic for the given consumer.

- Manager: `RM_down.py`

  - **Register producer**: Registers a producer and verifies if the producer is registered successfully or not.

  - **Register consumer**: Registers a consumer and verifies if the consumer is registered successfully or not.

  - **Produce to topic**: Produces messages to a topic and verifies if the messages are produced successfully or not.

  - **Make Read Manager 1 down and consume some messages**: Tests if the system can continue to consume messages when Read Manager 1 is down.

  - **Produce to topic again**: Produces messages to the topic again to test the availability of the system after Read Manager 1 is down.

  - **Make Read Manager 2 down and try to consume some messages**: Tests if the system can continue to consume messages when Read Manager 2 is down.


