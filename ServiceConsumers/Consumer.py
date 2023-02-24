import requests
# When broker is down: block hoke baith jao

class MyConsumer:
    def __init__(self, topics, broker):
        self.topics = topics
        self.base_url = broker
        self.topics_to_consumer_ids = {}

        for topic_name in topics:
            self.subscribe_to_topic(topic_name)

    def get_next(self, topic_name):

        if topic_name not in self.topics_to_consumer_ids:
            print(f"Please register to {topic_name}")
            return

        send_url = self.base_url + "/consumer/consume"
        data = {
            "topic": topic_name,
            "consumer_id": self.topics_to_consumer_ids[topic_name],
        }
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

    def get_topics(self):
        topics_url = self.base_url + "/topics"
        data = {"topic": topic_name}

        try:
            r = requests.get(topics_url, json=data)
            r.raise_for_status()
            response = r.json()
            if response["status"] == "Success":
                print("Active Topics : ")
                for topic_name in response["topics"]:
                    print(topic_name)
            return response
        except requests.exceptions.HTTPError as errh:
            print("Http Error:", errh)
            return {"status": "Failed", "message": "Http Error"}
        except requests.exceptions.ConnectionError as errc:
            print("Error Connecting:", errc)
            return {"status": "Failed", "message": "Error Connecting"}

    def subscribe_to_topic(self, topic_name):
        if topic_name in self.topics_to_consumer_ids.keys():
            print(f"Consumer already registered to {topic_name}")
            return
        register_url = self.base_url + "/consumer/register"
        data = {"topic": topic_name}
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
