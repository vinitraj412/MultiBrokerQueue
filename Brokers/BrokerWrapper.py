from flask import Flask, request
from Broker import LoggingQueue
from flask_migrate import Migrate
from BrokerModels import db

import uuid
import argparse

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = "postgresql://postgres:postgres@127.0.0.1:5430/postgres"
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
 
db.init_app(app)
migrate = Migrate(app, db)

broker = LoggingQueue()

# TODO : Add database schemas

@app.route('/')
def hello_world():
	return "<h1> Hello WOrld wow</h1>"

# todo: add topic name and partition id endpoint

@app.route("/producer/produce", methods=["POST"])
def enqueue():
	dict = request.get_json()
	topic = (dict['topic'])
	partition_id = (dict['partition_id'])
	message = dict['message']

	status = broker.enqueue(message=message, topic=topic, partition_id=partition_id)
	response = {}

	if status == 1:
		response["status"] = "Success"
	else:
		response["status"] = "Failure"

	return response

@app.route("/consumer/consume", methods=["GET"])
def dequeue():
	dict = request.get_json()
	topic = (dict['topic'])
	consumer_id = uuid.UUID(dict['consumer_id'])
	partition_id = (dict['partition_id'])
	offset = (dict['offset'])
	# if topic exists send consumer id
	status = broker.dequeue(topic_name= topic, partition_id= partition_id, offset= offset)
	response = {}

	if isinstance(status, str) :
		response["status"] = "Success"
		response["message"] = status
	else:
		response["status"] = "Failure"
		if status == -1:
			response["message"] = f"Topic {topic} does not exist."
		elif status == -2:
			response["message"] = f"Consumer {consumer_id} is not registered for topic {topic}."
		elif status == -3:
			response["message"] = f"No more messages for {consumer_id}"

	return response

@app.route("/size", methods=["GET"])
def size():
	dict = request.get_json()
	topic = (dict['topic'])
	partition_id = (dict['partition_id'])
	offset = (dict['offset'])

	status = broker.size(topic_name= topic,partition_id=partition_id, offset= offset)
	response = {}

	if status >=0 :
		response["status"] = "Success"
		response["size"] = status
	else:
		response["status"] = "Failure"
		if status == -1:
			response["message"] = f"Topic {topic} does not exist."
		elif status == -2:
			response["message"] = f"Consumer is not registered for topic {topic}."

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