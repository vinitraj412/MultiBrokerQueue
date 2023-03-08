from ...myqueue import MyConsumer
HOST = "127.0.0.1"
PORT = 5000
base_url = f"http://{HOST}:{PORT}"
from time import sleep
import random
c3 = MyConsumer(topics=["T-1", "T-3"], broker=base_url)

while True:
    print(c3.get_next("T-1"))
    sleep(random.uniform(0, 0.5))
    print(c3.get_next("T-3"))
    sleep(random.uniform(0, 0.5))
    