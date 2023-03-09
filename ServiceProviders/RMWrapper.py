# TODO: Implement Flask Interface \
from flask import Flask, request
from ReadManager import ReadManager
from flask_migrate import Migrate
from ManagerModel import db

import uuid
import argparse
import os

app = Flask(__name__)
DATABASE_CONFIG = {
    'driver': 'postgresql',
    'host': os.getenv('HOST_NAME'),
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
	return "<h1> Read Manager welcomes you!</h1>"

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

# @app.route("/consumer/register", methods=["POST"])
# def register_consumer():
# 	dict = request.get_json()
# 	topic = (dict['topic_name'])
# 	partition_id = dict.get('partition_id', None)
# 	status = ReadManager.register_consumer(topic, partition_id)

# 	response = {}
# 	if isinstance(status, int):
# 		response["status"] = "Failure"
# 		response["message"] = f"Consumer failed to register for topic {topic}."
# 	else:
# 		response["status"] = "Success"
# 		response["consumer_id"] = status
# 	return response

@app.route("/consumer/consume", methods=["GET"])
def dequeue():
	dict = request.get_json()
	topic = (dict['topic_name'])
	consumer_id = str(dict['consumer_id'])
	partition_id = dict.get('partition_id', None)		
    # if topic exists send consumer id
	response = ReadManager.dequeue(topic_name=topic, consumer_id=consumer_id, partition_id=partition_id)
	
	return response

@app.route("/size", methods=["GET"])
def size():
	dict = request.get_json()
	topic = (dict['topic_name'])
	consumer_id = str(dict['consumer_id'])
	partition_id = dict.get('partition_id', None)

	status = ReadManager.size(consumer_id=consumer_id, topic_name=topic, partition_id=partition_id)
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
	
	# app.run(debug=True, port = args.port)
	app.run(host='0.0.0.0')
	# TODO: create a thread that periodically sends heartbeat to manager