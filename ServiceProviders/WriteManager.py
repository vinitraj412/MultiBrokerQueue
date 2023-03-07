# import tea, coffee whatever
from ManagerModel import BrokerMetadata, ProducerMetadata, PartitionMetadata, ConsumerMetadata
import uuid
import requests
from typing import List
from concurrent.futures import ThreadPoolExecutor
from random import randint

class WriteManager:
    def __init__(self) -> None:
        pass
    
    # functions:

    # send_beat() {regulary sends beat to load balanacer, other managers using separate thread}
    # recv_beat() {from brokers}
        
    
    # create_topic(topic_name)
    # def create_topic(topic_name): may be Partion is also needed
    def receive_heartbeat(broker_id):
        BrokerMetadata.updateTimeStamp(broker_id)

    @staticmethod
    def create_topic(topic_name: str) -> List[int]:
        """
        Create a topic with the given name.
        We create partitions in all active brokers on demand 
        Args:
            topic_name (str): Name of the topic to be created
        Returns:
            int: 0 if topic is created successfully, -1 otherwise
        """
        broker_ids = BrokerMetadata.get_active_brokers()
        partition_ids = []
        for broker_id in broker_ids:
            partition_id = PartitionMetadata.createPartition(topic_name, broker_id)
            partition_ids.append(partition_id)

        return partition_ids

    @staticmethod
    def inc_offset(topic_name, consumer_id):
        ConsumerMetadata.incrementOffset(topic_name,consumer_id)

    @staticmethod
    def getBalancedPartition(topic_name):
        # active_brokers = BrokerMetadata.get_active_brokers()

        partition_ids = PartitionMetadata.listPartitions(topic_name)
        if(len(partition_ids)==0):
            return -1
        n = len(partition_ids)
        # get the corresponding broker for each partition
        idx = randint(0, n)
        for i in range(0, n):
            partition_id = partition_ids[(i+idx) %n]
            if(BrokerMetadata.checkBroker(PartitionMetadata.getBrokerID(topic_name, partition_id))):
                return partition_id
        # No ok partitions available
        return -1
    
    # register_producer(topic_name, parition_id = None) -> success ack
    @staticmethod
    def round_robin_partition(topic_name, producer_id):
        # Check if pro
        return WriteManager.getBalancedPartition(topic_name)

    # list_partitions(topic_name)
    def list_partitions(topic_name):
        return PartitionMetadata.listPartitions(topic_name)
    # returns partitioned list 

    def register_producer(topic_name):
        # check for existence of topic_name and partition_id
        # TODO: complete this
        producer_id = str(uuid.uuid4())
        ProducerMetadata.registerProducer(producer_id, topic_name)
        return producer_id
    
    def register_consumer(self, topic_name, partition_id=None):
        if partition_id is None:
            partition_id = self.getBalancedPartition(topic_name)
            if partition_id == -1:
                print("No partitions found")
                return -1

        consumer_id=str(uuid.uuid4())
        ConsumerMetadata.registerConsumer(consumer_id=consumer_id, topic_name=topic_name, partition_id=partition_id)
        return consumer_id

    # register_broker(broker_id) -> broker_id
    #   {broker gives its broker_id if it restarts after failure, else supply broker_id}
    def register_broker(endpoint):
        # todo: when adding a broker get it in sync with current topics and create partitions for it.
        prev = BrokerMetadata.getBrokerId(endpoint)
        if prev != -1:
            return prev
        
        try:
            broker_id = BrokerMetadata.createBroker(endpoint)
            topics = PartitionMetadata.listTopics()
            for topic in topics:
                PartitionMetadata.createPartition(topic,broker_id)
            print(f"Created Broker: {broker_id}")
            # import ipdb; ipdb.set_trace()
            return broker_id
        except Exception as e:
            # import ipdb; ipdb.set_trace()
            return -1
            # pass # TODO: add errors here baad mein
        

    # def send_heartbeat(endpoint):
    #     requests.post(endpoint,data="")

    

    # enqueue(topic_name, producer_id, message) -> success ack
    #   {use global msg_id, select broker, generate/select paritition_id}
    #   {creating new partitions on existing brokers}
    

    @staticmethod
    def send_request(broker_endpoint, topic_name, partition_id, message):
        data = {
            "topic_name": topic_name,
            "partition_id": partition_id,
            "message": message
        }
        response = requests.post(broker_endpoint, json=data)
        return response.json()

    @staticmethod
    def enqueue(producer_id, message, partition_id = None):
        topic_name = ProducerMetadata.getTopic(producer_id)
        if not ProducerMetadata.topic_registered(producer_id, topic_name):
            return {"status": "Failure", "message": "Producer not registered for this topic"}   
        
        if partition_id is None:
            partition_id = WriteManager.round_robin_partition(topic_name, producer_id)
        
        broker_id = PartitionMetadata.getBrokerID(topic_name, partition_id)
        broker_endpoint = BrokerMetadata.getBrokerEndpoint(broker_id)
        broker_endpoint = broker_endpoint + "/producer/produce"
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(WriteManager.send_request, broker_endpoint, topic_name, partition_id, message)
            response = future.result()
        
        return response
        # return WriteManager.send_request(broker_endpoint, topic_name, partition_id, message)
  
    # list_topics()
    def list_topics():
        return PartitionMetadata.listTopics()
    
    

    ## Write Ahead Logging (TODO Later)
    # General Flow : Receive a request -> log the transaction with enough info to restore
    #                -> interact with broker -> change state of transaction -> sync with other managers
    #                -> commit changes to DB -> delete trasaction_log
