# TODO: Implement Flask Interface \
from flask import Flask, request
from .WriteManager import WriteManager
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from .ManagerModel import db

import uuid
import argparse

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = "postgresql://postgres:postgrespassword@127.0.0.1:5432/flasksql"
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db.init_app(app)
migrate = Migrate(app, db)


# TODO : Add database schemas

@app.route('/')
def hello_world():
	return "<h1> Write Manager welcomes you!</h1>"

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
		# print(dict['topic_name'])
		# TODO : Interact with logging queue and return valid response 
		
		# return "test", 205
	
	else:
		# TODO : Return topic list
		return {"topics" : WriteManager.list_topics()}




@app.route("/producer/produce", methods=["POST"])
def enque():
	dict = request.get_json()
	topic = (dict['topic'])
	producer_id = uuid.UUID(dict['producer_id'])
	message = dict['message']

	
	status = broker.enqueue(topic_name=topic, producer_id=producer_id, message=message)
	response = {}

	if status == 1:
		response["status"] = "Success"
	else:
		response["status"] = "Failure"
		if status == -1:
			response["message"] = f"Topic {topic} does not exist."
		elif status == -2:
			response["message"] = f"Producer {producer_id} is not registered for topic {topic}."

	return response
	# else return error
	# return {"status"  : "failure",
	# 		  "message" : "topic not found"}

@app.route("/consumer/consume", methods=["GET"])
def dequeue():
	dict = request.get_json()
	topic = (dict['topic'])
	consumer_id = uuid.UUID(dict['consumer_id'])
	# if topic exists send consumer id
	status = broker.dequeue(topic_name=topic, consumer_id=consumer_id)
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
	# else return error
	# return {"status"  : "failure",
	# 		  "message" : "topic not found"}

@app.route("/size", methods=["GET"])
def size():
	dict = request.get_json()
	topic = (dict['topic'])
	consumer_id = uuid.UUID(dict['consumer_id'])

	status = broker.size(topic_name= topic, consumer_id= consumer_id)
	response = {}

	if status >=0 :
		response["status"] = "Success"
		response["size"] = status
	else:
		response["status"] = "Failure"
		if status == -1:
			response["message"] = f"Topic {topic} does not exist."
		elif status == -2:
			response["message"] = f"Consumer {consumer_id} is not registered for topic {topic}."

	return response


def register_broker(manager_ip, manager_port) -> str:
	# 1. Check if broker exists
	# 2. If not, register broker
	pass

def cmdline_args():
	# create parser
	parser = argparse.ArgumentParser()

	parser.add_argument("-i", "--ip", help="ip address", type=str, default="127.0.0.1")
	parser.add_argument("-p", "--port", help="port number", type=int, default=8080)

	parser.add_argument("-mi", "--manager_ip", help="manager ip address", type=str)
	parser.add_argument("-mp", "--manager_port", help="manager port number", type=int)

	return parser.parse_args()

if __name__ == '__main__':
	args = cmdline_args()

	# global broker
	with app.app_context():
		db.create_all() # <--- create db object.
	
	app.run(debug=True, port = args.port)
	register_broker(args.manager_ip, args.manager_port)

	# TODO: create a thread that periodically sends heartbeat to manager