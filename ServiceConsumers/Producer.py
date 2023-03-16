import requests


class MyProducer:
    def __init__(self, topics, broker):
        self.base_url = broker

        self.topics = topics
        self.topics_producer_id_map = {}

        for topic_name in self.topics:
            self.add_topic(topic_name)
    
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
        
    def send(self, topic_name, message, partition_id = None):
        if topic_name not in self.topics_producer_id_map.keys():
            print(f"Please register to {topic_name}")
            return
        send_url = self.base_url + "/producer/produce"
        data = {
            "topic_name": topic_name,
            "producer_id": self.topics_producer_id_map[topic_name],
            "message": message
        }
        if partition_id is not None:
            data["partition_id"] = partition_id
        # TODO : add an infinite loop until success
        try:
            r = requests.post(send_url, json=data)
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

    def add_topic(self, topic_name):
        if topic_name in self.topics_producer_id_map.keys():
            return
        register_url = self.base_url + "/producer/register"
        data = {"topic_name": topic_name}
        try:
            r = requests.post(register_url, json=data)
            r.raise_for_status()
            response = r.json()
            if response["status"] == "Success":
                producer_id = response["producer_id"]
                self.topics_producer_id_map[topic_name] = producer_id
            return response
        except requests.exceptions.HTTPError as errh:
            print("Http Error:", errh)
            return {"status": "Failed", "message": "Http Error"}
        except requests.exceptions.ConnectionError as errc:
            print("Error Connecting:", errc)
            return {"status": "Failed", "message": "Error Connecting"}


if __name__ == "__main__":
    try:
        topics = ["topic1", "topic2"]
        url = "http://localhost:8080"
        my_producer = MyProducer(topics=topics, broker=url)
        my_producer.add_topic("topic3")

    except Exception as e:
        print(e)

    finally:
        print(f"Producer created with topics: {topics}")
