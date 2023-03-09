from ServiceConsumers import MyConsumer
from time import sleep
import random

HOST = "10.147.197.95"
PORT = 8081
base_url = f"http://{HOST}:{PORT}"
#  Regitering to partition 0

sleep(5)
c3 = MyConsumer(topics=["T-1", "T-3"], broker=base_url, partition_ids = [0, None])

for _ in range(50):
    print(c3.get_next("T-1"))
    sleep(random.uniform(0, 0.5))
    print(c3.get_next("T-3"))
    sleep(random.uniform(0, 0.5))
    