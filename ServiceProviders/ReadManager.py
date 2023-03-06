from ManagerModel import PartitionMetadata, ConsumerMetadata, BrokerMetadata
import uuid
import requests
from concurrent.futures import ThreadPoolExecutor

class ReadManager:
    def __init__(self, endpoint_list) -> None:
        self.endpoint_list=endpoint_list
    
    # functions:

    # send_beat() {regulary sends beat to load balanacer, other managers using separate thread}
    
    # def send_heartbeat(self,endpoint):
    #     requests.post(endpoint,data="")

    # def beat(self):
    #     with ThreadPoolExecutor(len(self.endpoint_list)) as executor:
    #         futures = [executor.submit(self.send_heartbeat, endpoint) for endpoint in self.endpoint_list]
    

    def getBalancedPartition(self, topic_name):
        partitions = self.list_partitions(topic_name)
        if(len(partitions)==0):
            return -1
        # select partition with least number of consumers
        partition_id = min(partitions,key=lambda x: ConsumerMetadata.getConsumerCount(topic_name,x))
        return partition_id
    
    def getHealthyPartition(self, topic_name, consumer_id):
        partitions = self.list_partitions(topic_name)

        # select partition with offset < partition size 
        partitions = [part_id for part_id in partitions 
                      if ConsumerMetadata.getOffset(topic_name,consumer_id,part_id) < PartitionMetadata.getSize(topic_name,part_id)]
        if(len(partitions)==0):
            return -1
        partition_id = min(partitions,key=lambda x: ConsumerMetadata.getConsumerCount(topic_name,x))
        return partition_id
    
    # register_consumer(topic_name, parition_id = None) -> success ack

    def register_consumer(self, topic_name,partition_id=None):
        if partition_id is None:
            partition_id = self.getBalancedPartition(topic_name)
            if partition_id == -1:
                print("No partitions found")
                return -1

        consumer_id=str(uuid.uuid4())
        ConsumerMetadata.registerConsumer(consumer_id=consumer_id,topic_name=topic_name,partition_id=partition_id)
        return consumer_id
    
    def size(self,consumer_id,topic_name, partition_id=None):
        if partition_id is None:
            # return size of all partitions
            partitions = self.list_partitions(topic_name)
            size = 0
            for part_id in partitions:
                size += PartitionMetadata.getSize(topic_name,part_id) - ConsumerMetadata.getOffset(topic_name,consumer_id,part_id)
            return size
        return PartitionMetadata.getSize(topic_name,partition_id) - ConsumerMetadata.getOffset(topic_name,consumer_id,partition_id)

    def send_request(self, broker_endpoint, topic_name, partition_id, offset):
        data = {
            "topic_name": topic_name,
            "partition_id": partition_id,
            "offset": offset
        }
        response = requests.get(broker_endpoint, data=data)
        return response.json()

    def dequeue(self, consumer_id, topic_name, partition_id=None):
        if partition_id is None:
            partition_id = ReadManager.getHealthyPartition(self,topic_name,consumer_id)

        offset = ConsumerMetadata.getOffset(topic_name,consumer_id,partition_id)
        broker_id = PartitionMetadata.getBrokerID(topic_name,partition_id,offset)

        broker_endpoint = BrokerMetadata.getBrokerEndpoint(broker_id)
        broker_endpoint = broker_endpoint + "/consumer/consume"

        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(self.send_request, broker_endpoint, topic_name, partition_id, offset)
            ConsumerMetadata.incrementOffset(topic_name,consumer_id)  

        return future.result()
        # return output of async req
  
    # list_topics()
    def list_topics():
        return PartitionMetadata.listTopics()
    
    # list_partitions(topic_name)    
    def list_partitions(topic_name):
        return PartitionMetadata.listPartitions(topic_name)
	