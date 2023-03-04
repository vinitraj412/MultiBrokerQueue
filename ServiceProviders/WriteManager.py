# import tea, coffee whatever
from ManagerModel import BrokerMetadata, ProducerMetadata, PartitionMetadata, ConsumerMetadata
import uuid
import requests
from typing import List
from concurrent.futures import ThreadPoolExecutor

class WriteManager:
    def __init__(self) -> None:
        pass
    
    # functions:

    # send_beat() {regulary sends beat to load balanacer, other managers using separate thread}
    # recv_beat() {from brokers}
    # TODO ^
    
    # create_topic(topic_name)
    # def create_topic(topic_name): may be Partion is also needed
    
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
    def getBalancedPartition(topic_name):
        partitions = PartitionMetadata.listPartitions(topic_name)
        if(len(partitions)==0):
            return -1
        # select partition with least number of consumers
        partition_id = min(partitions,key=lambda x: ConsumerMetadata.getConsumerCount(topic_name,x))
        return partition_id
    
    # register_producer(topic_name, parition_id = None) -> success ack
    @staticmethod
    def round_robin_partition(topic_name, producer_id):
        # Check if pro
        return PartitionMetadata.getBalancedPartition(topic_name)

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

    # register_broker(broker_id) -> broker_id
    #   {broker gives its broker_id if it restarts after failure, else supply broker_id}
    def register_broker(endpoint):
        # todo: when adding a broker get it in sync with current topics and create partitions for it.
        try:
            broker_id = BrokerMetadata.createBroker(endpoint)
            print("Created Broker: {}",broker_id)
            return broker_id
        except:
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
            "topic": topic_name,
            "partition": partition_id,
            "message": message
        }
        response = requests.post(broker_endpoint, data=data)
        return response.json()

    @staticmethod
    def enqueue(producer_id, topic_name, message, partition_id = None):
        if not ProducerMetadata.topic_registered(producer_id, topic_name):
            return -1
        
        if partition_id is None:
            partition_id = WriteManager.round_robin_partition(topic_name, producer_id)
        
        broker_id = PartitionMetadata.getBrokerID(topic_name, partition_id)
        broker_endpoint = BrokerMetadata.getBrokerEndpoint(broker_id)

        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(WriteManager.send_request, broker_endpoint, topic_name, partition_id, message)
            response = future.result()
        
        return response
  
    # list_topics()
    def list_topics():
        return PartitionMetadata.listTopics()
    
    

    ## Write Ahead Logging (TODO Later)
    # General Flow : Receive a request -> log the transaction with enough info to restore
    #                -> interact with broker -> change state of transaction -> sync with other managers
    #                -> commit changes to DB -> delete trasaction_log

    def receive_heartbeat():
        pass