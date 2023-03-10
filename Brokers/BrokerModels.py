from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()

class ID(db.Model):
    broker_id = db.Column(db.Integer, primary_key=True)

    def __init__(self,broker_id):
        self.broker_id = broker_id

    @staticmethod
    def createID(broker_id):
        id = ID(broker_id)
        try:
            db.session.add(id)
            db.session.commit()
        except:
            db.session.rollback()
            return -1
    
    @staticmethod
    def getID():
        if (ID.query.first() == None):
            return -1
        return ID.query.first().broker_id

class TopicName(db.Model):
    __tablename__ = 'TopicName'
    topic_name = db.Column(db.String(), primary_key=True)
    partition_id = db.Column(db.Integer, primary_key=True)

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
        try:
            db.session.add(topic)
            db.session.commit()
        except:
            db.session.rollback()
            return -1

    @staticmethod
    def CheckTopic(topic_name, partition_id):
        topic = TopicName.query.filter_by(
            topic_name=topic_name, partition_id=partition_id).first()
        return True if topic else False


class TopicMessage(db.Model):
    __tablename__ = 'TopicMessage'

    id = db.Column(db.Integer, primary_key=True)
    topic_name = db.Column(db.String())
    partition_id = db.Column(db.Integer)
    message = db.Column(db.String())

    def __init__(self, topic_name, partition_id, message):
        self.topic_name = topic_name
        self.partition_id = partition_id
        self.message = message

    @staticmethod
    def addMessage(message, topic_name, partition_id):
        if not TopicName.CheckTopic(topic_name=topic_name, partition_id=partition_id):
            print(f"Topic {topic_name} with partition {partition_id} does not exist.")
            return -1

        topic = TopicMessage(topic_name, partition_id, message)
        try:
            db.session.add(topic)
            db.session.commit()
        except Exception as e:
            print(e)
            db.session.rollback()
            return -1
        return 1

    @staticmethod
    def retrieveMessage(topic_name, partition_id, offset):
        left_messages = TopicMessage.getSizeforTopic(
            topic_name, partition_id, offset)
        if (left_messages <= 0):
            return -1
        data = TopicMessage.query.filter_by(
            topic_name=topic_name, partition_id=partition_id).order_by(TopicMessage.id).offset(offset).first()
        assert data.message is not None, "Message is None"
        return data.message

    @staticmethod
    def getSizeforTopic(topic_name, partition_id, offset):
        print(type(partition_id))
        # offset is 0-indexed
        return TopicMessage.query.filter_by(topic_name=topic_name, partition_id=partition_id).count() - offset

    def __repr__(self):
        return f"{self.id} {self.topic_name} {self.producer_id} {self.message}"