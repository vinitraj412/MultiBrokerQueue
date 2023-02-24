from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()


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
    def addMessage(topic_name, producer_id, message):
        # check if topic exists
        if not TopicName.CheckTopic(topic_name):
            raise Exception("Topic does not exist")

        topic = TopicMessage(topic_name, producer_id, message)
        db.session.add(topic)
        db.session.commit()

    @staticmethod
    def retrieveMessage(topic_name, partition_id, message_id):
        left_messages = TopicMessage.getSizeforTopic(topic_name, partition_id)

        if (left_messages <= 0):
            return -1
        data = TopicMessage.query.filter_by(
            topic_name=topic_name, partition_id=partition_id, message_id=message_id)
        return data.message

    @staticmethod
    def getSizeforTopic(topic_name, offset):
        # offset is 0-indexed
        return TopicMessage.query.filter_by(topic_name=topic_name).count() - offset

    def __repr__(self):
        return f"{self.id} {self.topic_name} {self.producer_id} {self.message}"

class TopicOffsets(db.Model):
    __tablename__ = 'TopicOffsets'
    consumer_id = db.Column(db.String(), primary_key=True)
    topic_name = db.Column(db.String(), db.ForeignKey('TopicName.topic_name'))
    offset = db.Column(db.Integer)

    def __init__(self, consumer_id, topic_name):
        self.consumer_id = consumer_id
        self.topic_name = topic_name
        self.offset = 0

    @staticmethod
    def getOffset(consumer_id):
        return TopicOffsets.query.filter_by(consumer_id=consumer_id).first().offset

    @staticmethod
    def IncrementOffset(consumer_id):
        offset = TopicOffsets.getOffset(consumer_id)
        TopicOffsets.query.filter_by(consumer_id=consumer_id).update(
            {TopicOffsets.offset: offset + 1})
        db.session.commit()

    @staticmethod
    def getTopicName(consumer_id):
        return TopicOffsets.query.filter_by(consumer_id=consumer_id).first().topic_name

    @staticmethod
    def registerConsumer(consumer_id, topic_name):
        if not TopicName.CheckTopic(topic_name):
            raise Exception("Topic does not exist")
        consumer = TopicOffsets(consumer_id, topic_name)
        db.session.add(consumer)
        db.session.commit()

    @staticmethod
    def checkConsumer(consumer_id):
        return TopicOffsets.query.filter_by(consumer_id=consumer_id).count() != 0

    def __repr__(self):
        return f"{self.consumer_id} {self.topic_name} {self.offset}"

class TopicProducer(db.Model):
    __tablename__ = 'TopicProducer'
    producer_id = db.Column(db.String(), primary_key=True)
    topic_name = db.Column(db.String())

    def __init__(self, producer_id, topic_name):
        self.producer_id = producer_id
        self.topic_name = topic_name

    @staticmethod
    def registerProducer(producer_id, topic_name):  
        if not TopicName.CheckTopic(topic_name):
            raise Exception("Topic does not exist")
        producer = TopicProducer(producer_id, topic_name)
        db.session.add(producer)
        db.session.commit()

    @staticmethod
    def checkProducer(producer_id):
        return TopicProducer.query.filter_by(producer_id=producer_id).count() != 0

    def __repr__(self):
        return f"{self.producer_id} {self.topic_name}"

def return_objects():
    return TopicProducer, TopicMessage, TopicName, TopicOffsets