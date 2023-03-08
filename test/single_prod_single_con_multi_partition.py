import requests

from time import sleep


def test(HOST, PORT):

    base_url = f"http://{HOST}:{PORT}"
    print(f"Hitting the base url: {base_url}")
    
    num_of_messages = 3

    print("Testing the endpoint create Topic")
    url = base_url + "/topics"
    data = {
        "topic_name": "topic_1"
    }

    try:
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

    print()
    
    print("Testing the endpoint create Topic (duplicate topic)")
    url = base_url + "/topics"
    data = {
        "topic_name": "topic_1"
    }

    try:
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


    print()
    print("Testing the endpoint get topics")
    url = base_url + "/topics"


    try:
        r = requests.get(url)
        r.raise_for_status()
        response = r.json()
        print("Response")
        print(response)
    except requests.exceptions.HTTPError as errh:
        print("Http Error:", errh)

    except requests.exceptions.ConnectionError as errc:
        print("Error Connecting:", errc)


    print()
    print("Testing the endpoint register consumer")
    url = base_url + "/consumer/register"
    data = {
        "topic_name": "topic_1"
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
    print("Testing the endpoint register consumer (invalid topic name)")
    url = base_url + "/consumer/register"
    data = {
        "topic_name": "topic_2"
    }

    try:
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


    print()
    print("Testing the endpoint register producer (existing topic)")
    url = base_url + "/producer/register"
    data = {
        "topic_name": "topic_1"
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

    print()
    print("Testing the endpoint register producer (new topic)")
    url = base_url + "/producer/register"
    data = {
        "topic_name": "topic_2"
    }
    try:
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

    print()
    print("calling the endpoint get Topics")
    url = base_url + "/topics"


    try:
        r = requests.get(url)
        r.raise_for_status()
        response = r.json()
        print("Response")
        print(response)
    except requests.exceptions.HTTPError as errh:
        print("Http Error:", errh)

    except requests.exceptions.ConnectionError as errc:
        print("Error Connecting:", errc)

    print("Note : Topic topic_2 was created while registering the producer.")


    print()
    print(f"Pushing {num_of_messages} messages on Topic tp1")


    url = base_url + "/producer/produce"


    try:
        for i in range(1,num_of_messages+1):
            data = {
                "topic_name": "topic_1",
                "producer_id": producer_id,
                "message": f"LOG MESSAGE {i}"
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


    print()

    print(f"Consuming messages using the consumer {consumer_id} and reporting size for {num_of_messages+1} times")


    try:
        data = {
            "topic_name": "topic_1",
            "consumer_id": consumer_id,
        }
        print(f"request = {data}")
        for i in range(num_of_messages+1):
            print()

            url = url = base_url + "/size"
            print(f"Size of topic_1 for given consumer")
            r = requests.get(url, json=data)
            print(r)
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
    
    print("Testing produce after all messages are consumed")
    
    url = base_url + "/producer/produce"

    try:
        data = {
            "topic_name": "topic_1",
            "producer_id": producer_id,
            "message": f"LOG MESSAGE NEW"
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

    print("Testing size after new message is produced")
    
    data = {
        "topic_name": "topic_1",
        "consumer_id": consumer_id,
    }
    print(f"request = {data}")
    print()

    url = url = base_url + "/size"
    print(f"Size of topic_1 for given consumer")
    r = requests.get(url, json=data)
    print(r)
    response = r.json()
    print("Response")
    print(response)
    
    print()
        
    print("Testing consume after new message is produced")
    url = base_url + "/consumer/consume"
    
    r = requests.get(url, json=data)
    r.raise_for_status()
    response = r.json()
    print("Response")
    print(response)
    
    
if __name__ == "__main__":
    HOST = "10.147.197.95"
    PORT = 8081
    test(HOST, PORT)