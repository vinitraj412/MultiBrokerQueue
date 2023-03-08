from ManagerModel import PartitionMetadata, ConsumerMetadata, BrokerMetadata
import uuid
import random
import requests
from concurrent.futures import ThreadPoolExecutor

class ReadManager:
    @staticmethod
    def getHealthyPartition( topic_name, consumer_id):

        # active_brokers = BrokerMetadata.get_active_brokers()

        partition_ids = PartitionMetadata.listPartition_IDs(topic_name)
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
    def send_request(broker_endpoint, topic_name, partition_id, consumer_id, offset):
        data = {
            "topic_name": topic_name,
            "consumer_id": consumer_id,
            "partition_id": partition_id,
            "offset": offset
        }
        response = requests.get(broker_endpoint, json=data)
        return response.json()
    
    @staticmethod
    def inc_offset(wm_endpoint, topic_name, consumer_id,partition_id):
        data = {
            "topic_name": topic_name,
            "consumer_id": consumer_id,
            "partition_id":partition_id
        }
        response = requests.post(wm_endpoint, json=data)
        return response.json()

    # @staticmethod
    # def update_consumer_part_req(wm_endpoint,consumer_id, new_part_metadata):
    #     data = {
    #         "consumer_id": consumer_id,
    #         "new_part_metadata":new_part_metadata
    #     }
    #     response = requests.post(wm_endpoint, json=data)
    #     return response.json()

    @staticmethod
    # TODO: is partition_id necessary isnt partition fixed when registering 
    def dequeue(consumer_id, topic_name, partition_id=None):
        if partition_id is None:
            partition_id = ReadManager.getHealthyPartition(topic_name, consumer_id)
            if partition_id == -1:
                response_dict = {'status': 'Failure',
                                'message': 'No healthy partitions found'}
                return response_dict
            # new_part_metadata=PartitionMetadata.getPartition_Metadata(topic_name=topic_name,partition_id=partition_id)
            # ConsumerMetadata.updateConsumerPartition(consumer_id=consumer_id,new_partition_metadata=new_part_metadata)
            # ReadManager.update_consumer_part_req("http://write_manager:5000/consumer/update_partition_metadata", consumer_id,new_part_metadata)

        offset = ConsumerMetadata.getOffset(topic_name, consumer_id, partition_id)
        broker_id = PartitionMetadata.getBrokerID(topic_name, partition_id)
        
        broker_endpoint = BrokerMetadata.getBrokerEndpoint(broker_id)
        broker_endpoint = broker_endpoint + "/consumer/consume"

        res= ReadManager.send_request( broker_endpoint, topic_name, partition_id,consumer_id, offset)
            # ConsumerMetadata.incrementOffset(topic_name,consumer_id) # send request to WM instead
        if res['status']=='Success':
            ReadManager.inc_offset("http://write_manager:5000/consumer/offset", topic_name, consumer_id,partition_id)
        return res
        # return output of async req
  
    # list_topics()
    @staticmethod
    def list_topics():
        return PartitionMetadata.listTopics()
    
    # list_partitions(topic_name)    
    @staticmethod
    def list_partitions(topic_name):
        return PartitionMetadata.listPartition_IDs(topic_name)
	