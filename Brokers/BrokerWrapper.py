from flask import Flask, request
from Broker import LoggingQueue
from flask_migrate import Migrate
from BrokerModels import db, ID

from concurrent.futures import ThreadPoolExecutor
import socket
import uuid
import argparse
from random import randint
from time import sleep
import requests
from threading import Thread
import os
app = Flask(__name__)

DATABASE_CONFIG = {
    'driver': 'postgresql',
    'host': os.getenv('DB_NAME'),
    'user': 'postgres',
    'password': 'postgres',
    'port': 5432,
    'dbname': os.getenv('DB_NAME')
}
db_url = f"{DATABASE_CONFIG['driver']}://{DATABASE_CONFIG['user']}:{DATABASE_CONFIG['password']}@{DATABASE_CONFIG['host']}:{DATABASE_CONFIG['port']}/{DATABASE_CONFIG['dbname']}"
app.config['SQLALCHEMY_DATABASE_URI'] = db_url

app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db.init_app(app)
migrate = Migrate(app, db)

broker = LoggingQueue()

# TODO : Add database schemas

broker_id = None


@app.route('/')
def hello_world():
    return "<h1> Hello WOrld wow</h1>"

# todo: add topic name and partition id endpoint


@app.route("/producer/produce", methods=["POST"])
def enqueue():
    print("produce")
    dict = request.get_json()
    print(dict)
    topic = dict['topic_name']
    partition_id = dict['partition_id']
    message = dict['message']
    # import ipdb; ipdb.set_trace()
    status = broker.enqueue(message=message, topic=topic,
                            partition_id=partition_id)
    response = {}

    if status == 1:
        response["status"] = "Success"
    else:
        response["status"] = "Failure"

    return response


@app.route("/consumer/consume", methods=["GET"])
def dequeue():
    dict = request.get_json()
    topic = (dict['topic_name'])
    consumer_id = str(dict['consumer_id'])
    partition_id = (dict['partition_id'])
    offset = (dict['offset'])
    # if topic exists send consumer id
    status = broker.dequeue(
        topic_name=topic, partition_id=partition_id, offset=offset)
    response = {}

    if isinstance(status, str):
        response["status"] = "Success"
        response["message"] = status
    else:
        response["status"] = "Failure"
        if status == -1:
            response["message"] = f"Topic {topic} does not exist."
        elif status == -3:
            response["message"] = f"Consumer {consumer_id} is not registered for topic {topic}."
        elif status == -2:
            response["message"] = f"No more messages for {consumer_id}"

    return response


@app.route("/size", methods=["GET"])
def size():
    dict = request.get_json()
    topic = (dict['topic_name'])
    partition_id = (dict['partition_id'])
    offset = (dict['offset'])

    status = broker.size(
        topic_name=topic, partition_id=partition_id, offset=offset)
    response = {}

    if status >= 0:
        response["status"] = "Success"
        response["size"] = status
    else:
        response["status"] = "Failure"
        if status == -1:
            response["message"] = f"Topic {topic} does not exist."
        elif status == -2:
            response["message"] = f"No message in topic {topic}."

    return response


def register(mIP, mPort, p):
    # /broker/register
    # response["status"] = "Success"
    # response["message"] = status this is the broker id
    send_url = f"http://{mIP}:{mPort}/broker/register"
    data = {
        "port": p
    }
    try:
        r = requests.post(send_url, json=data)
        r.raise_for_status()
        response = r.json()
        if response["status"] == "Success":
            print("Registered successfully")
            return response["broker_id"]
        else:
            print(f"Failed, {response['message']}")
            return -1

    except requests.exceptions.HTTPError as errh:
        print("Http Error:", errh)
        return -1
    except requests.exceptions.ConnectionError as errc:
        print("Error Connecting:", errc)
        return -1

# python BrokerWrapper.py -p 8082 -mIP 127.0.0.1 -mPort 8080


def cmdline_args():
    # create parser
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--port", help="port number",
                        type=int, default=5000)
    parser.add_argument("-mIP", "--managerIP",
                        help="write manager IP address", type=str, default="write_manager")
    parser.add_argument("-mPort", "--managerPort",
                        help="write manager port number", type=int, default=5000)
    # parser.add_argument("-mIP", "--managerIP",
    #                     help="read manager IP address", type=str, default="read_manager")
    # parser.add_argument("-mPort", "--managerPort",
    #                     help="read manager port number", type=int, default=8081)
    return parser.parse_args()


if __name__ == '__main__':
    args = cmdline_args()

    # global broker
    with app.app_context():
        db.create_all()  # <--- create db object.
        broker_id = ID.getID()
        if (broker_id == -1):
            hostname = socket.gethostname()
            ip_address = socket.gethostbyname(hostname)
            print(f"IP Address: {ip_address}, Port: {args.port}")

            # keep on trying to connect to manager
            while True:
                response = register(args.managerIP, args.managerPort, args.port)
                # import ipdb
                # ipdb.set_trace()
                if response != -1:
                    broker_id = response
                    ID.createID(broker_id)
                    break
                sleep(randint(1, 3)/100)

    # with ThreadPoolExecutor(max_workers=1) as executor:
    #     executor.submit(broker.heartbeat, args.managerIP, args.managerPort, broker_id)
    executor = Thread(target=broker.heartbeat,args=(args.managerIP, args.managerPort, broker_id,args.port))
    executor.daemon = True
    executor.start()
    app.run(host='0.0.0.0',port=args.port)

    # TODO remove reloader = false if needed
    # app.run(debug=True, port=args.port, use_reloader=False)
    # executor.join()
    # TODO: create a thread that periodically sends heartbeat to manager
