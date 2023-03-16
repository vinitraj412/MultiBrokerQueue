# TESTING 

Firstly, We have conducted the 3 consumers - 5 producers test that was prescribed in the previous assignment before conducting more focussed tests. The Newly designed Messaging system worked as expected during testing

## Test Files and Tests Conducted

`broker_down.py` \
tests conducted in the file :


`RM_down.py` \
tests conducted in the file :



`single_prod_single_con_multi_partition.py` \
(NOTE : This file also tests the single partition production)
tests conducted in the file :

- **Test the endpoint create Topic**: To test if a new topic can be created on the server.

- **Test the endpoint create Topic (duplicate topic)**: To test if the server responds correctly when a duplicate topic is created.

- **Test the endpoint get topics**: To test if the server responds correctly when a request to get all topics is made.

- **Test the endpoint register consumer**: To test if the server responds correctly when a consumer is registered for an existing topic.

- **Test the endpoint register consumer (invalid topic name)**: To test if the server responds correctly when a consumer is registered for an invalid topic name.

- **Test the endpoint register producer (existing topic)**: To test if the server responds correctly when a producer is registered for an existing topic.

- **Test the endpoint register producer (new topic)**: To test if the server responds correctly when a producer is registered for a new topic.

- **Calling the endpoint get Topics**: To test if the server responds correctly when a request to get all topics is made.

- **Pushing messages on a Topic**: To test if messages can be pushed on a topic on the server.


`single_prod_single_con_single_partition.py` \
tests conducted in the file :
- **Endpoint register producer (new topic)**: This test is responsible for registering a new producer for a new topic.
- **Production to general partition (randomized)**: This test is responsible for producing random messages to a general partition of the topic.
- **List partitions for a topic**: This test is responsible for listing all partitions of a given topic.
- **Production to specific partition**: This test is responsible for producing random messages to a specific partition of the topic.
- **Endpoint register consumer (topic level)**: This test is responsible for registering a new consumer at the topic level.
- **Consuming messages using the consumer and reporting size for 2*num_of_messages times**: This test is responsible for consuming messages using the registered consumer at the topic level, and reporting the size of the topic after each consume request.
- **Endpoint register consumer (partition level)**: This test is responsible for registering a new consumer at the partition level.
- **Consuming messages using the consumer from partition and reporting size**: This test is responsible for consuming messages using the registered consumer at the partition level, and reporting the size of the partition after each consume request.