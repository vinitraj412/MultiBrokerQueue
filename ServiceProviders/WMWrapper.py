# TODO: Implement Flask Interface \
from flask import Flask, request
from WriteManager import WriteManager
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from ManagerModel import db
import os
import uuid
import argparse

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


# TODO : Add database schemas
@app.route('/')
def hello_world():
	return "ip: {}, port: {}".format(request.environ['REMOTE_ADDR'],request.environ['REMOTE_PORT'])

@app.route("/topics", methods=["POST", "GET"])
def topics():
	print(request.method)
	if request.method == "POST":
		dict = request.get_json()
		topic_name = dict['topic_name']
			
		response = {}
		partition_ids = WriteManager.create_topic(topic_name)
		if len(partition_ids) >=0:
			response["status"] = "Success"
			response["message"] = f"Topic {topic_name} created successfully!"
		else:
			response["status"] = "Failure"
			response["message"] = f"No brokers available"

		return response
		# TODO : Interact with logging queue and return valid response 
	
	else:
		# TODO : Return topic list
		return {"topics" : WriteManager.list_topics()}

@app.route("/topics/partitions", methods=["GET"])
def partitions():
	dict = request.get_json()
	topic_name = dict['topic_name']
	
	response = {}
	partitions = WriteManager.list_partitions(topic_name)
	if partitions is not None:
		response["status"] = "Success"
		response["partitions"] = partitions
	else:
		response["status"] = "Failure"
		response["message"] = f"Topic {topic_name} does not exist."
	
	return response

@app.route("/producer/register", methods=["POST"])
def register_producer():
	dict = request.get_json()
	topic = (dict['topic_name'])
	status = WriteManager.register_producer(topic)

	response = {}
	if isinstance(status, int):
		response["status"] = "Failure"
		response["message"] = f"Producer failed to register for topic {topic}."
	else:
		response["status"] = "Success"
		response["producer_id"] = status
	return response

@app.route("/broker/receive_beat", methods=["POST"])
def receive_beat():
	req = request.get_json()
	# print("Heartbeat from : ", end=" ")
	# print(req)
	broker_id = req["broker_id"]
	WriteManager.receive_heartbeat(broker_id)
	return {}

@app.route("/broker/register", methods=["POST"])
def register_broker():
	ip = request.environ['REMOTE_ADDR']
	# port = request.environ['REMOTE_PORT']
	dict = request.get_json()
	port = dict['port']
	endpoint = "http://{}:{}".format(ip,port)
	status = WriteManager.register_broker(endpoint)
	response = {}
	
	if status == -1:
		response["status"] = "Failure"
		response["message"] = f"Broker {endpoint} failed to register."
	else:
		response["status"] = "Success"
		response["message"] = status
		response["broker_id"] = status

	return response

@app.route("/producer/produce", methods=["POST"])
def enqueue():
	dict = request.get_json()
	# topic = (dict['topic_name'])
	producer_id = str(dict['producer_id'])
	
	partition_id = dict.get('partition_id', None)
	message = dict['message']

	status = WriteManager.enqueue(producer_id=producer_id, partition_id=partition_id, message=message)
	response = {}
	# import ipdb; ipdb.set_trace()
	if status == 1:
		response["status"] = "Success"
	else:
		response["status"] = "Failure"
		if status == -1:
			response["message"] = f"producer {producer_id} not registered to {topic}"
		elif status == -2:
			response["message"] = f"Producer {producer_id} is not registered for topic {topic}."

	return response


def cmdline_args():
	# create parser
	parser = argparse.ArgumentParser()
	parser.add_argument("-p", "--port", help="port number", type=int, default=8080)
	return parser.parse_args()

if __name__ == '__main__':
	args = cmdline_args()

	# global broker
	with app.app_context():
		db.create_all() # <--- create db object.
	
	app.run(debug=True, port = args.port)
	# TODO: create a thread that periodically sends heartbeat to manager