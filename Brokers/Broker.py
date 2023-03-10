# TODO: Implement the Distributed Queue using the Individual Topic Queues
import BrokerModels
import threading
import uuid
from typing import Dict, List, Tuple, Set
from concurrent.futures import ThreadPoolExecutor
from BrokerModels import db, TopicName, TopicMessage
import requests
from time import sleep
# TODO: define enum for success and failure codes


class LoggingQueue:
    def __init__(self):
        pass

    def heartbeat(self, ip: str, port: int, broker_id, self_port) -> None:
        data = {"broker_id": broker_id, "port": self_port}
        send_url = f"http://{ip}:{port}/broker/receive_beat"

        while True:
            try:
                r = requests.post(send_url, json=data)
                # print("sending beat")
                r.raise_for_status()
            except requests.exceptions.HTTPError as errh:
                print("Http Error:", errh)
            except requests.exceptions.ConnectionError as errc:
                print("Error Connecting:", errc)
            sleep(0.05)

    def create_topic(self, topic_name: str, partition_id) -> None:
        if not TopicName.CheckTopic(topic_name=topic_name, partition_id=partition_id):
            TopicName.CreateTopic(topic_name=topic_name,
                                  partition_id=partition_id)
            print(f"Topic {topic_name} with partition {partition_id} created.")
            return 1
        else:
            print(
                f"Topic {topic_name} with partition {partition_id} already exists.")
            return -1

    def list_topics(self) -> List[Tuple[str, str]]:
        topic_part_list = TopicName.ListTopics()
        return topic_part_list

    def enqueue(self, message: str, topic: str, partition_id: int) -> int:
        # check if (topic, partition_id) exists else create it
        if not TopicName.CheckTopic(topic_name=topic, partition_id=partition_id):
            TopicName.CreateTopic(topic_name=topic, partition_id=partition_id)
            print(f"Topic {topic} with partition {partition_id} created.")

        status = TopicMessage.addMessage(
            topic_name=topic, partition_id=partition_id, message=message)
        if status == 1:
            print(
                f"Message '{message}' added to topic {topic} with partition {partition_id}.")
            return 1
        else:
            print(
                f"Message '{message}' could not be added to topic {topic} with partition {partition_id}.")
            return -1

    def dequeue(self, topic_name: str, partition_id: int, offset: int, *args, **kwargs) -> str:

        if not TopicName.CheckTopic(topic_name=topic_name, partition_id=partition_id):
            print(
                f"Topic {topic_name} with partition {partition_id} does not exist.")
            return -1

        message = TopicMessage.retrieveMessage(topic_name=topic_name, partition_id=partition_id,
                                               offset=offset)
        if (isinstance(message, str)):
            print(
                f"Message '{message}' from topic {topic_name}, partition {partition_id}.")
            return message
        elif message == -1:
            print(f"No message in queue!!!")
            return -2

    def size(self, topic_name: str, partition_id: str, offset) -> int:
        if not TopicName.CheckTopic(topic_name=topic_name, partition_id=partition_id):
            print(
                f"Topic {topic_name} with partition {partition_id} does not exist.")
            return -1
        return TopicMessage.getSizeforTopic(topic_name=topic_name, partition_id=partition_id, offset=offset)

    def set_details(self, broker_id, ip, port):
        self.DetailsDB.addBrokerDetails(broker_id=broker_id, ip=ip, port=port)

    def get_details(self):
        broker = self.DetailsDB.getBrokerDetails()
        if broker == -1:
            print("No broker details found.")
        return broker   # -1 if broker does not exist
