# TODO: Implement Flask Interface \
from flask import Flask, request
from ReadManager import ReadManager
from flask_migrate import Migrate
from ManagerModel import db

import uuid
import argparse

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = "postgresql://postgres:postgres@127.0.0.1:5432/postgres"
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db.init_app(app)
migrate = Migrate(app, db)


# TODO : Add database schemas

@app.route('/')
def hello_world():
	return "<h1> Write Manager welcomes you!</h1>"

@app.route("/topics", methods=["GET"])
def topics():
	print(request.method)
	# TODO : Return topic list
	return {"topics" : ReadManager.list_topics()}

@app.route("/topics/partitions", methods=["GET"])
def partitions():
	dict = request.get_json()
	topic_name = dict['topic_name']
	
	response = {}
	partitions = ReadManager.list_partitions(topic_name)
	if partitions is not None:
		response["status"] = "Success"
		response["partitions"] = partitions
	else:
		response["status"] = "Failure"
		response["message"] = f"Topic {topic_name} does not exist."
	
	return response

@app.route("/consumer/consume", methods=["GET"])
def dequeue():
	dict = request.get_json()
	topic = (dict['topic'])
	consumer_id = uuid.UUID(dict['consumer_id'])
	partition_id = dict.get('partition_id', None)		
    # if topic exists send consumer id
	status = ReadManager.dequeue(topic_name=topic, consumer_id=consumer_id, partition_id=partition_id)
	response = {}

    # TODO: check the async io output
	if isinstance(status, str):
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
	topic = (dict['topic_name'])
	consumer_id = str(dict['consumer_id'])

	status = ReadManager.size(consumer_id=consumer_id, topic_name=topic)
	response = {}

	# using status as a flag to check the size returned
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

def cmdline_args():
	# create parser
	parser = argparse.ArgumentParser()

	parser.add_argument("-i", "--ip", help="ip address", type=str, default="127.0.0.1")
	parser.add_argument("-p", "--port", help="port number", type=int, default=8081)

	parser.add_argument("-mi", "--manager_ip", help="manager ip address", type=str)
	parser.add_argument("-mp", "--manager_port", help="manager port number", type=int)

	return parser.parse_args()

if __name__ == '__main__':
	args = cmdline_args()

	# global broker
	with app.app_context():
		db.create_all() # <--- create db object.
	
	app.run(debug=True, port = args.port)
	# TODO: create a thread that periodically sends heartbeat to manager