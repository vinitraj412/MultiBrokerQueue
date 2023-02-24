import requests


class MyProducer:
    def __init__(self, topics, broker):
        self.base_url = broker

        self.topics = topics
        self.topics_producer_id_map = {}
        for topic in self.topics:

            self.add_topic(topic)
            # r = requests.post(register_url, json = data)

            # register producer id for data
            # print(r.json())

    def send(self, topic_name, message):
        if topic_name not in self.topics_producer_id_map.keys():
            print(f"Please register to {topic_name}")
            return
        send_url = self.base_url + "/producer/produce"
        data = {
            "topic": topic_name,
            "producer_id": self.topics_producer_id_map[topic_name],
            "message": message
        }
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
        data = {"topic": topic_name}
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
        url = "http://localhost:5000"
        my_producer = MyProducer(topics=topics, broker=url)
        my_producer.add_topic("topic3")

    except Exception as e:
        print(e)

    finally:
        print(f"Producer created with topics: {topics}")
