# TODO: Implement Flask Interface \
from flask import Flask, request
from Partition import LoggingQueue
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from PartitionModels import db, TopicName

import json
import uuid
app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = "postgresql://postgres:postgrespassword@127.0.0.1:5432/flasksql"
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
 
db.init_app(app)
migrate = Migrate(app, db)

loggingQueue = LoggingQueue()

# TODO : Add database schemas

@app.route('/')
def hello_world():
	return "<h1> Hello WOrld wow</h1>"

@app.route("/topics", methods=["POST", "GET"])
def topics():
	print(request.method)
	# print(loggingQueue.__dict__)
	# return "hi"
	if request.method == "POST":
		dict = request.get_json()
		topic = dict['topic_name']
		response = {}
		status = loggingQueue.create_topic(topic)
		if status == 1:
			response["status"] = "Success"
			response["message"] = f"Topic {topic} created successfully!"
		else:
			response["status"] = "Failure"
			response["message"] = f"Topic {topic} already exists!"
		
		return response
		# print(dict['topic_name'])
		# TODO : Interact with logging queue and return valid response 
		
		# return "test", 205
	
	else:
		# TODO : Return topic list
		return {"topics" : loggingQueue.list_topics()}

@app.route("/consumer/register", methods=["POST"])
def register_consumer():
	dict = request.get_json()	
	print(dict['topic'])
	topic = dict['topic']
	
	status = loggingQueue.register_consumer(topic)
	response=  {}
	if status == -1:
		response["status"] = "Failure"
		response["message"] = f"Topic {topic} doesn't exist!"
	else:
		response["status"] = "Success"
		response["consumer_id"] = status
	return response
	# if topic exists send consumer id
	# return {"status" : "success",
	#         "consumer_id" : 1234}
	# else return error
	# return {"status"  : "failure",
	# 		  "message" : "topic not found"}

@app.route("/producer/register", methods=["POST"])
def register_producer():
	dict = request.get_json()
	print(dict['topic'])
	topic = dict['topic']
	
	status = loggingQueue.register_producer(topic)
	response=  {}
	response["status"] = "Success"
	response["producer_id"] = status
	return response
	# else return error
	# return {"status"  : "failure",
	# 		  "message" : "topic not found"}


@app.route("/producer/produce", methods=["POST"])
def enque():
	dict = request.get_json()
	topic = (dict['topic'])
	producer_id = uuid.UUID(dict['producer_id'])
	message = dict['message']

	
	status = loggingQueue.enqueue(topic_name=topic, producer_id=producer_id, message=message)
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
	status = loggingQueue.dequeue(topic_name=topic, consumer_id=consumer_id)
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

	status = loggingQueue.size(topic_name= topic, consumer_id= consumer_id)
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
	# if topic exists send consumer id
	# return {"status" : "success"}
	# else return error
	# return {"status"  : "failure",
	# 		  "message" : "topic not found"}

if __name__ == '__main__':
	# global loggingQueue
	with app.app_context():
		db.create_all() # <--- create db object.
	app.run(debug=True)