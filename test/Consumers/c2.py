from ...ServiceConsumers import MyConsumer
HOST = "10.147.197.95"
PORT = 8081
base_url = f"http://{HOST}:{PORT}"
from time import sleep
import random

c2 = MyConsumer(topics=["T-1",  "T-3"], broker=base_url)

while True:
    print(c2.get_next("T-1"))
    sleep(random.uniform(0, 0.5))
    print(c2.get_next("T-3"))
    sleep(random.uniform(0, 0.5))
    