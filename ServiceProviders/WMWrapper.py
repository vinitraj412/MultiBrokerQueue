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
		# TODO : Interact with logging queue and return valid response 
	
	else:
		# TODO : Return topic list
		return {"topics" : WriteManager.list_topics()}

@app.route("/producer/register", methods=["POST"])
def register_producer():
	dict = request.get_json()
	topic = (dict['topic'])
	status = WriteManager.register_producer(topic)

	response = {}
	if isinstance(status, int):
		response["status"] = "Failure"
		response["message"] = f"Producer failed to register for topic {topic}."
	else:
		response["status"] = "Success"
		response["producer_id"] = status
	return response


@app.route("/broker/register", methods=["POST"])
def register_broker():
	dict = request.get_json()
	endpoint = (dict['endpoint'])
	status = WriteManager.register_broker(endpoint)
	response = {}
	
	if isinstance(status, int):
		response["status"] = "Failure"
		response["message"] = f"Broker {endpoint} failed to register."
	else:
		response["status"] = "Success"
		response["message"] = status

	return response

@app.route("/producer/produce", methods=["POST"])
def enqueue():
	dict = request.get_json()
	topic = (dict['topic'])
	producer_id = uuid.UUID(dict['producer_id'])
	partition_id = dict.get('partition_id', None)
	message = dict['message']

	status = WriteManager.enqueue(topic_name=topic, producer_id=producer_id, partition_id=partition_id, message=message)
	response = {}

	if status == 1:
		response["status"] = "Success"
	else:
		response["status"] = "Failure"
		if status == -1:
			response["message"] = f"Topic {topic} for producer {producer_id} and partition {partition_id} does not exist."
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