from ManagerModel import PartitionMetadata, ConsumerMetadata, BrokerMetadata
import uuid
import random
import requests
from concurrent.futures import ThreadPoolExecutor

class ReadManager:
    @staticmethod
    def getHealthyPartition( topic_name, consumer_id):

        # active_brokers = BrokerMetadata.get_active_brokers()

        partition_ids = PartitionMetadata.listPartitions(topic_name)
        if(len(partition_ids)==0):
            return -1
        n = len(partition_ids)
        # get the corresponding broker for each partition
        idx = random.randint(0, n)
        for i in range(n):
            partition_id = partition_ids[(i+idx) %n]
            if(BrokerMetadata.checkBroker(PartitionMetadata.getBrokerID(topic_name, partition_id)) and (ReadManager.size(consumer_id, topic_name, partition_id)>0)):
                return partition_id
        
        return -1
    
    @staticmethod
    def size(consumer_id,topic_name, partition_id=None):
        if partition_id is None:
            # return size of all partitions
            partitions = ReadManager.list_partitions(topic_name)
            size = 0
            for part_id in partitions:
                size += PartitionMetadata.getSize(topic_name, part_id) - ConsumerMetadata.getOffset(topic_name, consumer_id, part_id)
            return size
        return PartitionMetadata.getSize(topic_name, partition_id) - ConsumerMetadata.getOffset(topic_name, consumer_id, partition_id)

    @staticmethod
    def send_request(broker_endpoint, topic_name, partition_id, offset):
        data = {
            "topic_name": topic_name,
            "partition_id": partition_id,
            "offset": offset
        }
        response = requests.get(broker_endpoint, data=data)
        return response.json()
    
    @staticmethod
    def inc_offset(wm_endpoint, topic_name, consumer_id):
        data = {
            "topic_name": topic_name,
            "consumer_id": consumer_id
        }
        response = requests.post(wm_endpoint, data=data)
        return response.json()

    @staticmethod
    def dequeue(consumer_id, topic_name, partition_id=None):
        if partition_id is None:
            partition_id = ReadManager.getHealthyPartition(topic_name, consumer_id)
            if partition_id == -1:
                response_dict = {'status': 'Failure',
                                'message': 'No healthy partitions found'}
                return response_dict

        offset = ConsumerMetadata.getOffset(topic_name, consumer_id, partition_id)
        broker_id = PartitionMetadata.getBrokerID(topic_name, partition_id)
        
        broker_endpoint = BrokerMetadata.getBrokerEndpoint(broker_id)
        broker_endpoint = broker_endpoint + "/consumer/consume"

        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(ReadManager.send_request, broker_endpoint, topic_name, partition_id, offset)
            # ConsumerMetadata.incrementOffset(topic_name,consumer_id) # send request to WM instead
            ReadManager.inc_offset("write_manager:5000/consumer/offset", topic_name, consumer_id)

        return future.result()
        # return output of async req
  
    # list_topics()
    @staticmethod
    def list_topics():
        return PartitionMetadata.listTopics()
    
    # list_partitions(topic_name)    
    @staticmethod
    def list_partitions(topic_name):
        return PartitionMetadata.listPartitions(topic_name)
	