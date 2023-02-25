# TODO: Implement the Distributed Queue using the Individual Topic Queues
import BrokerModels
import threading
import uuid
from typing import Dict, List, Tuple, Set
from concurrent.futures import ThreadPoolExecutor

# TODO: define enum for success and failure codes   

class LoggingQueue:
    def __init__(self):
        self.DetailsDB, self.NameDB, self.MessageDB = BrokerModels.return_objects()

    def create_topic(self, topic_name: str, partition_id) -> None:
        if not self.NameDB.CheckTopic(topic_name=topic_name, partition_id=partition_id):
            self.NameDB.CreateTopic(topic_name=topic_name, partition_id=partition_id)
            print(f"Topic {topic_name} with partition {partition_id} created.")
            return 1 
        else:
            print(f"Topic {topic_name} with partition {partition_id} already exists.")
            return -1

    def list_topics(self) -> List[str]:
        topic_list = self.NameDB.ListTopics()
        print(topic_list)
        # import ipdb; ipdb.set_trace()
        return list(topic_list)

    def enqueue(self, message_id:int, topic_name:str, partition_id:str, producer_id: int, message: str) -> int:
        
        producer_id = str(producer_id)
        if not self.NameDB.CheckTopic(topic_name=topic_name, partition_id=partition_id):
            print(f"Topic {topic_name} with partition {partition_id} does not exist.")
            return -1
        
        # self.topics[topic_name].add_log(message,message_metadata=f"{producer_id}")
        self.MessageDB.addMessage(message_id=message_id, topic_name=topic_name, partition_id=partition_id, producer_id=producer_id, message=message)
        print(f"Producer {producer_id} enqueued message '{message}' to topic {topic_name} with partition {partition_id}.")
        return 1

    def dequeue(self, message_id: int, topic_name: str, partition_id: str) -> str:

        if not self.NameDB.CheckTopic(topic_name=topic_name, partition_id=partition_id):
            print(f"Topic {topic_name} with parititon {partition_id} does not exist.")
            return -1
        
        message = self.MessageDB.retrieveMessage(message_id=message_id, topic_name=topic_name, partition_id=partition_id)

        if message == -1:
            print(f"No message in queue!!!")
            return -2
        else:
            print(f"Message '{message}' from topic {topic_name}, partition {partition_id}.")
            return message

    def size(self, topic_name: str, partition_id: str) -> int:
        if not self.NameDB.CheckTopic(topic_name=topic_name, partition_id=partition_id):
            print(f"Topic {topic_name} with partition {partition_id} does not exist.")
            return -1
            
        return self.MessageDB.getSizeforTopic(topic_name=topic_name, partition_id=partition_id)
    
    def set_details(self, broker_id, ip, port):
        self.DetailsDB.addBrokerDetails(broker_id=broker_id, ip=ip, port=port)
    
    def get_details(self):
        broker = self.DetailsDB.getBrokerDetails()
        if broker == -1:
            print("No broker details found.")
        return broker   # -1 if broker does not exist