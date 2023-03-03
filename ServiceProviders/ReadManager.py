from .ManagerModel import ManagerMessageView, PartitionMetadata, ConsumerMetadata
import uuid
import requests
from concurrent.futures import ThreadPoolExecutor

class ReadManager:
    def __init__(self, endpoint_list) -> None:
        self.endpoint_list=endpoint_list
    
    # functions:

    # send_beat() {regulary sends beat to load balanacer, other managers using separate thread}
    
    def send_heartbeat(self,endpoint):
        requests.post(endpoint,data="")

    def beat(self):
        with ThreadPoolExecutor(len(self.endpoint_list)) as executor:
            futures = [executor.submit(self.send_heartbeat, endpoint) for endpoint in self.endpoint_list]
    
    # size(topic_name, consumer_id)
    def size(topic_name, consumer_id):
        TotalSize = 0
        partition_id = ConsumerMetadata.getPartitionId(topic_name = topic_name, consumer_id=consumer_id)
        offset = ConsumerMetadata.getOffset(topic_name=topic_name, consumer_id=consumer_id)
        if partition_id is not None:
            TotalSize =  ManagerMessageView.query.filter_by(topic_name=topic_name, partition_id=partition_id).count()
        else:
            TotalSize =  ManagerMessageView.query.filter_by(topic_name=topic_name).count()
        return TotalSize - offset

    
    # register_consumer(topic_name, parition_id = None) -> success ack
    def register_consumer(topic_name,partition_id=None):
        if partition_id is None:
            partitions=ReadManager.list_partitions(topic_name)
            if(len(partitions)==0):
                return -2
            partition_id=partitions[0]

        if not PartitionMetadata.exit(topic_name,partition_id):
            return -1

        consumer_id=str(uuid.uuid4())
        ConsumerMetadata.registerConsumer(consumer_id=consumer_id,topic_name=topic_name,partition_id=partition_id)
        return consumer_id

    # dequeue(topic_name, consumer_id) -> message
    #   {use messages table to find broker}
    def dequeue(topic_name, consumer_id):
        #TODO : RETURN status and message
        # find partition id and offset from ConsumerMetadata 
        # find broker id using partition id and offset from ManagerMessageView
        # increment offset

        partition_id = ConsumerMetadata.getPartitionId(topic_name,consumer_id)
        
        if partition_id is None:
            offset = ConsumerMetadata.getOffset(topic_name,consumer_id)
            broker_id,partition_id=ManagerMessageView.getBrokerIDGlobalOffset(topic_name,offset)
        else:
            offset=ConsumerMetadata.getOffset(topic_name,consumer_id)
            broker_id=ManagerMessageView.getBrokerID(topic_name,partition_id,offset)
        
        ## send async req to broker with broker id 

        ConsumerMetadata.incrementOffset(topic_name,consumer_id)  

        # return output of async req
  
    # list_topics()
    def list_topics():
        return PartitionMetadata.listTopics()
    
    # list_partitions(topic_name)    
    def list_partitions(topic_name):
        return PartitionMetadata.listPartitions(topic_name)


    ## Write Ahead Logging (TODO Later)
    # General Flow : Receive a request -> log the transaction with enough info to restore
    #                -> interact with broker -> change state of transaction -> sync with other managers
    #                -> commit changes to DB -> delete trasaction_log

	