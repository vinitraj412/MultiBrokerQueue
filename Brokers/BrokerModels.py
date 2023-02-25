from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()

class BrokerDetails(db.Model):
    __tablename__ = 'BrokerDetails'
    broker_id = db.Column(db.String(), primary_key=True)
    ip = db.Column(db.String())
    port = db.Column(db.Integer())

    def __init__(self, broker_id, ip, port) -> None:
        self.broker_id = broker_id
        self.ip = ip
        self.port = port
    
    def __repr__(self) -> str:
        return f"Broker id: {self.broker_id} IP: {self.ip} Port: {self.port}"
    
    @staticmethod
    def getBrokerDetails():
        broker = BrokerDetails.query.first()
        return broker if broker else -1
    
    def addBrokerDetails(broker_id, ip, port):
        broker = BrokerDetails(broker_id, ip, port)
        db.session.add(broker)
        db.session.commit()

class TopicName(db.Model):
    __tablename__ = 'TopicName'
    topic_name = db.Column(db.String(), primary_key=True)
    partition_id = db.Coumn(db.String(), primary_key=True)

    def __init__(self, topic_name, partition_id):
        self.topic_name = topic_name
        self.partition_id = partition_id

    def __repr__(self):
        return f"{self.topic_name} {self.partition_id}"

    @staticmethod
    def ListTopics():
        return [(topic.topic_name, topic.partition_id) for topic in TopicName.query.all()]

    @staticmethod
    def CreateTopic(topic_name, partition_id):
        topic = TopicName(topic_name, partition_id)
        db.session.add(topic)
        db.session.commit()

    @staticmethod
    def CheckTopic(topic_name, partition_id):
        topic = TopicName.query.filter_by(topic_name=topic_name,partition_id=partition_id).first()
        return True if topic else False

class TopicMessage(db.Model):
    __tablename__ = 'TopicMessage'
    id = db.Column(db.Integer, primary_key=True)
    message_id = db.Column(db.Integer)
    topic_name = db.Column(db.String(), db.ForeignKey('TopicName.topic_name'))
    partition_id = db.Coumn(db.String(), db.ForeignKey('TopicName.partition_id'))
    producer_id = db.Column(db.String())
    message = db.Column(db.String())

    def __init__(self, message_id, topic_name, partition_id, producer_id, message):
        self.message_id = message_id
        self.topic_name = topic_name
        self.partition_id = partition_id
        self.producer_id = producer_id
        self.message = message

    @staticmethod
    def addMessage(message_id, topic_name, partition_id, producer_id, message):
        # check if topic exists
        if not TopicName.CheckTopic(topic_name, partition_id):
            raise Exception("Topic does not exist")

        topic = TopicMessage(message_id, topic_name, partition_id, producer_id, message)
        db.session.add(topic)
        db.session.commit()

    @staticmethod
    def retrieveMessage(message_id, topic_name, partition_id):
        # left_messages = TopicMessage.getSizeforTopic(topic_name, partition_id)

        # if (left_messages <= 0):
        #     return -1
        data = TopicMessage.query.filter_by(
            topic_name=topic_name, partition_id=partition_id, message_id=message_id).first()
        return data.message if data.message else -1

    @staticmethod
    def getSizeforTopic(topic_name, partition_id):
        # offset is 0-indexed
        return TopicMessage.query.filter_by(topic_name=topic_name, partition_id=partition_id).count()

    def __repr__(self):
        return f"{self.id} {self.topic_name} {self.producer_id} {self.message}"


def return_objects():
    return BrokerDetails, TopicName, TopicMessage