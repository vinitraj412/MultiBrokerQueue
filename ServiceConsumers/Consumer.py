import requests
# When broker is down: block hoke baith jao

class MyConsumer:
    def __init__(self, topics, broker, partition_ids):
        self.topics = topics
        self.base_url = broker
        self.partition_ids = partition_ids
        self.topics_to_consumer_ids = {}

        for i, topic_name in enumerate(topics):
            self.subscribe_to_topic(topic_name, partition_ids[i])

    def list_topics(self, topic_name):
        send_url = self.base_url + '/topics'
        data = {
            "topic_name": topic_name
        }
        try:
            r = requests.get(send_url, json=data)
            r.raise_for_status()
            response = r.json()
            if response["status"] == "Success":
                print("Sent successfully")
            else:
                print(f"Failed, {response['message']}")
            return response
        except requests.exceptions.HTTPError as errh:
            print("Http Error:", errh)
            return {"status": "Failed", "message": "Http Error"}
        except requests.exceptions.ConnectionError as errc:
            print("Error Connecting:", errc)
            return {"status": "Failed", "message": "Error Connecting"}

    def list_partitions(self, topic_name):
        send_url = self.base_url + '/topics/partitions'
        data = {
            "topic_name": topic_name
        }
        try:
            r = requests.get(send_url, json=data)
            r.raise_for_status()
            response = r.json()
            if response["status"] == "Success":
                print("Sent successfully")
            else:
                print(f"Failed, {response['message']}")
            return response
        except requests.exceptions.HTTPError as errh:
            print("Http Error:", errh)
            return {"status": "Failed", "message": "Http Error"}
        except requests.exceptions.ConnectionError as errc:
            print("Error Connecting:", errc)
            return {"status": "Failed", "message": "Error Connecting"}

    def get_next(self, topic_name):

        if topic_name not in self.topics_to_consumer_ids:
            print(f"Please register to {topic_name}")
            return

        send_url = self.base_url + "/consumer/consume"
        data = {
            "topic_name": topic_name,
            "consumer_id": self.topics_to_consumer_ids[topic_name],
        }
        if self.partition_ids[self.topics.index(topic_name)] is not None:
            data["partition_id"] = self.partition_ids[self.topics.index(topic_name)]

        try:
            r = requests.get(send_url, json=data)
            r.raise_for_status()
            response = r.json()

            if response["status"] == "Success":
                print("Received successfully")
            else:
                print(f"Failed, {response['message']}")

            return response
        except requests.exceptions.HTTPError as errh:
            print("Http Error:", errh)
            return {"status": "Failed", "message": "Http Error"}
        except requests.exceptions.ConnectionError as errc:
            print("Error Connecting:", errc)
            return {"status": "Failed", "message": "Error Connecting"}

    def subscribe_to_topic(self, topic_name, partition_id=None):
        if topic_name in self.topics_to_consumer_ids.keys():
            print(f"Consumer already registered to {topic_name}")
            return
        register_url = self.base_url + "/consumer/register"
        data = {"topic_name": topic_name, "partition_id": partition_id}
        try:
            r = requests.post(register_url, json=data)
            r.raise_for_status()
            response = r.json()
            if response["status"] == "Success":
                consumer_id = response["consumer_id"]
                self.topics_to_consumer_ids[topic_name] = consumer_id
            return response
        except requests.exceptions.HTTPError as errh:
            print("Http Error:", errh)
            return {"status": "Failed", "message": "Http Error"}
        except requests.exceptions.ConnectionError as errc:
            print("Error Connecting:", errc)
            return {"status": "Failed", "message": "Error Connecting"}


if __name__ == "__main__":
    pass
