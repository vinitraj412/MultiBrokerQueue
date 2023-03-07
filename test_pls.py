import requests
HOST = "localhost"
PORT = 8080
base_url = f"http://{HOST}:{PORT}"

num_of_messages = 3

print("Testing the endpoint create Topic")
url = base_url + "/topics"
data = {
    "topic_name": "tp_1"
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
print("Testing the endpoint create Topic (topic already exists case)")
url = base_url + "/topics"
data = {
    "topic_name": "tp_1"
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
print("Testing the endpoint get Topics")
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
    "topic_name": "tp_1"
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
print("Testing the endpoint register consumer (case with invalid topic name)")
url = base_url + "/consumer/register"
data = {
    "topic_name": "tp_2"
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
print("Testing the endpoint register producer")
url = base_url + "/producer/register"
data = {
    "topic_name": "tp_1"
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
print("Testing the endpoint register producer (missing topic name)")
url = base_url + "/producer/register"
data = {
    "topic_name": "tp_2"
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

print("Note : Topic tp_2 was created while registering the producer.")


print()
print(f"Pushing {num_of_messages} messages on Topic tp_1")


url = base_url + "/producer/produce"


try:
    for i in range(1,num_of_messages+1):
        data = {
            "topic_name": "tp_1",
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
        "topic_name": "tp_1",
        "consumer_id": consumer_id,
    }
    print(f"request = {data}")
    for i in range(num_of_messages+1):
        print()

        url = url = base_url + "/size"
        print(f"Size of tp_1 for given consumer")
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
except requests.exceptions.HTTPError as errh:
    print("Http Error:", errh)

except requests.exceptions.ConnectionError as errc:
    print("Error Connecting:", errc)