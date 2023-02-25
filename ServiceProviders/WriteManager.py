# import tea, coffee whatever
from .ManagerModel import ManagerMessageView, BrokerMetadata, ProducerMetadata, PartitionMetadata
import uuid
import requests



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
    def create_topic(topic_name):
        # choose a broker, 
        
        
        # choice 1
        # with least number of messages
        # create a partition in that broker
        
        # select broker_id, count(message_id) from ManagerMessageView where topic_name = topic_name 
        # group by broker_id order by count(message_id) 
        broker_id = ManagerMessageView.getLeastMessageBroker(topic_name)
        if broker_id < 0:
            return -1
        partition_id = PartitionMetadata.createPartition(topic_name, broker_id)

        return partition_id
        # choice 2
        # with minimum number of partitions of the given topic_name


    # def
        

    # size(topic_name, partition_id = None)
    def size(topic_name, partition_id = None):
        if partition_id is not None:
            return ManagerMessageView.query.filter_by(topic_name=topic_name, partition_id=partition_id).count()
        else:
            return ManagerMessageView.query.filter_by(topic_name=topic_name).count()
    # this will use god table, query table then filter then count

    # register_producer(topic_name, parition_id = None) -> success ack
    def round_robin_partition(topic_name, producer_id):
        partition_offset = ProducerMetadata.query.filter_by(producer_id=producer_id).first().partition_offset
        num_partitions = PartitionMetadata.query.filter_by(topic_name=topic_name).count()
        partition_offset = (partition + 1) % num_partitions

        partition = PartitionMetadata.getPartition(topic_name, partition_offset)

        return partition        

    # list_partitions(topic_name)
    def list_partitions(topic_name):
        return PartitionMetadata.listPartitions(topic_name)

    def register_producer(topic_name, partition_id = None):
        # check for existence of topic_name and partition_id

        if partition_id is None:
        # choose the first topic partition
            partitions = WriteManager.list_partitions(topic_name)

            if(len(partitions) == 0):
                return -2
            partition_id = 0


        if not PartitionMetadata.exist(topic_name, partition_id):
            return -1
            # Topic.createTopic(topic_name, partition_id,)
          
        producer_id = str(uuid.uuid4())
        ProducerMetadata.registerProducer(producer_id, topic_name, partition_id)

        return producer_id

    # register_broker(broker_id) -> broker_id
    #   {broker gives its broker_id if it restarts after failure, else supply broker_id}
    def register_broker(endpoint):
        try:
            broker_id = BrokerMetadata.createBroker(endpoint)
            print("Created Broker: {}",broker_id)
        except:
            pass # TODO: add errors here baad mein
        

    # def send_heartbeat(endpoint):
    #     requests.post(endpoint,data="")

    

    # enqueue(topic_name, producer_id, message) -> success ack
    #   {use global msg_id, select broker, generate/select paritition_id}
    #   {creating new partitions on existing brokers}
    

    def enqueue(producer_id, topic_name, partition_offset, message):
        # if not PartitionMetadata.exist(topic_name, partition):
            # return -1
        if partition_offset == -1:
            partition_offset = ProducerMetadata.query.filter_by(producer_id=producer_id).first().partition_offset
            num_partitions = PartitionMetadata.query.filter_by(topic_name=topic_name).count()
            partition_offset = (partition + 1) % num_partitions

        partition = PartitionMetadata.getPartition(topic_name, partition_offset)
        message = ManagerMessageView(producer_id, topic_name, partition, message)
            # Topic.createTopic(topic_name, partition_id,)
          
        # producer_id = str(uuid.uuid4())
        # ProducerMetadata.registerProducer(producer_id, topic_name, partition_id)

        # return producer_id
  
    # list_topics()
    def list_topics():
        return PartitionMetadata.listTopics()
    
    

    ## Write Ahead Logging (TODO Later)
    # General Flow : Receive a request -> log the transaction with enough info to restore
    #                -> interact with broker -> change state of transaction -> sync with other managers
    #                -> commit changes to DB -> delete trasaction_log

    def receive_heartbeat():
        pass