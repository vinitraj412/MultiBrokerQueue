import requests

from time import sleep
import random

def test(HOST, PORT):

    base_url = f"http://{HOST}:{PORT}"
    print(f"Hitting the base url: {base_url}")
    
    num_of_messages = 10
    
    print("Testing the endpoint register producer (new topic)")
    url = base_url + "/producer/register"
    data = {
        "topic_name": "topic_8"
    }
    producer_id = None
    try:
        print(f"request = {data}")
        r = requests.post(url, json=data)
        r.raise_for_status()
        response = r.json()
        print("Response")
        producer_id = response["producer_id"]
        print(response)
    except requests.exceptions.HTTPError as errh:
        print("Http Error:", errh)

    except requests.exceptions.ConnectionError as errc:
        print("Error Connecting:", errc)
    
    print("Testing production to general partition (randomized)")
    
    url = base_url + "/producer/produce"
    print(f"Pushing {num_of_messages} messages on Topic topic_8")
    
    try:
        for i in range(num_of_messages):
            data = {
                "topic_name": "topic_8",
                "producer_id": producer_id,
                "message": f"LOG MESSAGE {i + 1}"
            }
            print(f"request = {data}")
            r = requests.post(url, json=data)
            r.raise_for_status()
            response = r.json()
            print("Response")
            print(response)
    except requests.exceptions.HTTPError as errh:
        print("Http Error:", errh)

    except requests.exceptions.ConnectionError as errc:
        print("Error Connecting:", errc)
    pass

    print("Testing list partitions for a topic")
    url = base_url + "/topics/partitions"
    
    data = {
        "topic_name": "topic_8"
    }
    
    r = requests.get(url, json=data)
    response = r.json()
    print("Response")
    print(response)
    partitions = response['partitions']
    
    url = base_url + "/producer/produce"
    print("Testing production to specific partition")
    for i in range(num_of_messages):
        partition_id = random.choice(partitions)
        data = {
            "topic_name": "topic_8",
            "producer_id": producer_id,
            "partition_id": partition_id,	
            "message": f"LOG MESSAGE {i + num_of_messages + 1}",
        }
        print(f"request = {data}")
        r = requests.post(url, json=data)
        r.raise_for_status()
        response = r.json()
        print("Response")
        print(response)

    # 20 messages -- 
    
    print()
    print("Testing the endpoint register consumer (topic level)")
    url = base_url + "/consumer/register"
    data = {
        "topic_name": "topic_8"
    }

    consumer_id = None
    try:
        print(f"request = {data}")
        r = requests.post(url, json=data)
        r.raise_for_status()
        response = r.json()
        print("Response")
        consumer_id = response['consumer_id']
        print(response)
    except requests.exceptions.HTTPError as errh:
        print("Http Error:", errh)

    except requests.exceptions.ConnectionError as errc:
        print("Error Connecting:", errc)
        
    print()

    print(f"Consuming messages using the consumer {consumer_id} and reporting size for {2*num_of_messages} times")

    try:
        data = {
            "topic_name": "topic_8",
            "consumer_id": consumer_id,
        }
        print(f"request = {data}")
        for i in range(num_of_messages * 2):
            print()

            url = url = base_url + "/size"
            print(f"Size of topic_8 for given consumer")
            r = requests.get(url, json=data)
            response = r.json()
            print("Response")
            print(response)
            
            print()
            print(f"Consuming messages")
            url = base_url + "/consumer/consume"
            
            r = requests.get(url, json=data)
            r.raise_for_status()
            response = r.json()
            print("Response")
            print(response)
            sleep(1)
    except requests.exceptions.HTTPError as errh:
        print("Http Error:", errh)

    except requests.exceptions.ConnectionError as errc:
        print("Error Connecting:", errc)
    
    print()
    print("Testing the endpoint register consumer (partition level)")
    url = base_url + "/consumer/register"
    partition_id = random.choice(partitions)
    data = {
        "topic_name": "topic_8",
        "partition_id": partition_id
    }

    consumer_id = None
    try:
        print(f"request = {data}")
        r = requests.post(url, json=data)
        r.raise_for_status()
        response = r.json()
        print("Response")
        consumer_id = response['consumer_id']
        print(response)
    except requests.exceptions.HTTPError as errh:
        print("Http Error:", errh)

    except requests.exceptions.ConnectionError as errc:
        print("Error Connecting:", errc)
        
    print()

    print(f"Consuming messages using the consumer {consumer_id} from partition {partition_id} and reporting size too")

    try:
        data = {
            "topic_name": "topic_8",
            "consumer_id": consumer_id,
            "partition_id": partition_id
        }
        print(f"request = {data}")
        while True:
            print()

            url = url = base_url + "/size"
            print(f"Size of topic_8 for given consumer")
            r = requests.get(url, json=data)
            response = r.json()
            print("Response")
            print(response)
            
            if response['size'] == 0:
                break
            
            print()
            print(f"Consuming messages")
            url = base_url + "/consumer/consume"
            
            r = requests.get(url, json=data)
            r.raise_for_status()
            response = r.json()
            print("Response")
            print(response)
            
            sleep(0.02)
            
    except requests.exceptions.HTTPError as errh:
        print("Http Error:", errh)

    except requests.exceptions.ConnectionError as errc:
        print("Error Connecting:", errc)


if __name__ == "__main__":
    HOST = "10.147.197.95"
    PORT = 8081
    test(HOST, PORT)