from flask_sqlalchemy import SQLAlchemy
from datetime import datetime
# from sortedcollections import OrderedSet

db = SQLAlchemy()

# Table : Brokers (maps the ip/port of each broker)
# [broker_id, endpoint, last_beat_timestamp]

# service registry
class BrokerMetadata(db.Model):
    __tablename__ = 'BrokerMetadata'

    broker_id = db.Column(db.Integer, primary_key=True)
    endpoint = db.Column(db.String(),unique=True)
    last_beat_timestamp = db.Column(db.DateTime,default=datetime.utcnow)
    status = db.Column(db.Boolean, default=True)
    

    def __init__(self,endpoint):
        self.endpoint = endpoint

    def __repr__(self) -> str:
        return "<Broker %r>" % self.endpoint

    @staticmethod
    def updateTimeStamp(endpoint):
        broker = BrokerMetadata.query.filter_by(endpoint=endpoint).first
        broker.last_beat_timestamp = datetime.utcnow()
        db.session.commit()

    @staticmethod
    def createBroker(endpoint) -> int:
        broker = BrokerMetadata(endpoint)
        db.session.add(broker)
        db.session.commit()
        return int(broker.broker_id)

    @staticmethod
    def updateStatus(endpoint: str, status: bool) -> None:
        broker = BrokerMetadata.query.filter_by(endpoint=endpoint).first()
        broker.status = status
        db.session.commit()

    @staticmethod
    def checkBroker(endpoint: str) -> bool:
        broker = BrokerMetadata.query.filter_by(endpoint=endpoint).first()
        return True if (broker is not None and broker.status) else False

# Table : Managers (maps the ip/port of other managers)
# [broker_id, endpoint, last_beat_timestamp]
class ManagerMetadata(db.Model):
    pass

# Table : Partitions (which broker has a particular partition)
# used in round_robin(or random) selection
# [topic_name, partition_id, broker_id]
class PartitionMetadata(db.Model):
    __tablename__ = 'PartitionMetadata'
    topic_name = db.Column(db.String(),)
    partition_id = db.Column(db.Integer(), primary_key=True)
    broker_id = db.Column(db.Integer(), db.ForeignKey('BrokerMetadata.broker_id'))
    
    def __init__(self, topic_name, broker_id):
        self.topic_name = topic_name
        self.broker_id = broker_id
    
    @staticmethod
    def createPartition(topic_name, broker_id):
        entry = PartitionMetadata(topic_name, broker_id)
        db.session.add(entry)
        db.session.commit()
        partition_id = entry.partition_id
        return partition_id
    
    @staticmethod
    def exist(topic_name, partition_id):
        if PartitionMetadata.query.filter_by(topic_name=topic_name, partition_id=partition_id).count() > 0:
            return True
        else:
            return False
    @staticmethod
    def listTopics():
        query = [partition.topic_name for partition in PartitionMetadata.query.all()]
        return list(set(query))
    
    @staticmethod
    def listPartitions(topic_name):
        query = [topic.partition_id for topic in PartitionMetadata.query.filter_by(topic_name=topic_name).all()]
        return sorted(list(set(query)))

    def getPartition(topic_name, offset):
        return PartitionMetadata.query.filter_by(topic_name=topic_name)[offset].partition_id

# Table : Messages (used for finding which broker has a certain message with an offset)
# [topic_name, id(increasing int), broker_id, partition_id]
# id is a global counter for all messages

class ManagerMessageView(db.Model):
    __tablename__ = 'ManagerMessageView'
    topic_name = db.Column(db.String(),)
    partition_id = db.Column(db.Integer(),db.ForeignKey('PartitionMetadata.partition_id')) # can this be NULL?
    broker_id = db.Column(db.Integer(),db.ForeignKey('BrokerMetadata.broker_id'))
    id = db.Column(db.Integer, primary_key=True)

    def __init__(self,topic_name,partition_id,broker_id):
        self.broker_id=broker_id
        self.topic_name=topic_name
        self.partition_id=partition_id

    @staticmethod
    def getBrokerID(targetTopic, targetPartitionId, targetOffset):
        return ManagerMessageView.query.filter_by(topic_name=targetTopic, partition_id = targetPartitionId)[targetOffset].broker_id
    
    def getBrokerIDGlobalOffset(targetTopic, targetOffset):
        entry=ManagerMessageView.query.filter_by(topic_name=targetTopic)[targetOffset]
        return entry.broker_id,entry.partition_id
    @staticmethod
    def addMessageMetadata(topic_name,partition_id, broker_id):
        ## check topic name????

        ## no need to check since foreign key reference
        # if not BrokerMetadata.checkBroker(): ## check if broker still up 
        #     raise Exception("Broker down")
        
        message_entry=ManagerMessageView(topic_name,partition_id,broker_id)
        try:
            db.session.add(message_entry)
            db.session.commit()
        except:
            db.session.rollback()
    
    @staticmethod
    def getLeastMessageBroker(topic_name):
        brokers = set([b.broker_id for b in BrokerMetadata.query.all()])
        
        query = ManagerMessageView.query.filter_by(topic_name=topic_name).all()
        broker_message_load = {b:0 for b in brokers}
        for q in query:
            broker_message_load[q] += 1
        if(len(q) > 0):
            return min(broker_message_load, key = broker_message_load.get)
        
        return -1


# Table : Offsets(self explanatory)  
# [Consumer_id, topic_name, partition_id(null if subscribed to entire topic), offset]
class ConsumerMetadata(db.Model):
    __tablename__ = 'ConsumerMetadata'
    consumer_id=db.Column(db.String(),primary_key=True)
    topic_name=db.Column(db.String())
    parition_id=db.Column(db.Integer(),db.ForeignKey('PartitionMetadata.partition_id'))
    offset=db.Column(db.Integer())

    def __init__(self,consumer,topic_name,partition_id,offset):
        self.consumer_id=consumer
        self.topic_name=topic_name
        self.parition_id=partition_id
        self.offset=offset
    
    @staticmethod
    def registerConsumer(consumer_id, topic_name, partition_id):
        entry=ConsumerMetadata(consumer_id,topic_name,partition_id,0)
        db.session.add(entry)
        db.session.commit()
    
    @staticmethod
    def getPartitionId(topic_name,consumer_id):
        return ConsumerMetadata.query.filter_by(topic_name=topic_name,consumer_id=consumer_id).first().partition_id
    
    @staticmethod
    def getOffset(topic_name,consumer_id):
        return ConsumerMetadata.query.filter_by(topic_name=topic_name,consumer_id=consumer_id).first().offset
    
    @staticmethod
    def incrementOffset(topic_name,consumer_id):
        offset = ConsumerMetadata.getOffset(topic_name,consumer_id)
        ConsumerMetadata.query.filter_by(topic_name=topic_name,consumer_id=consumer_id).update({ConsumerMetadata.offset: offset + 1})
        db.session.commit()


# Table : Producers
# [producer_id, topic_name, partition_id(null if publishing to entire topic)]
class ProducerMetadata(db.model):
    __tablename__ = 'ProducerMetadata'
    producer_id = db.Column(db.String(), primary_key=True)
    topic_name = db.Column(db.String())
    partition_offset = db.Column(db.Integer())

    def __init__(self,producer_id, topic_name, partition_offset):
        self.producer_id = producer_id
        self.topic_name = topic_name
        self.partition_offset = partition_offset
    
    @staticmethod
    def registerProducer(producer_id, topic_name, partition_offset=None):
        entry = ProducerMetadata(producer_id, topic_name, partition_offset)
        db.session.add(entry)
        db.session.commit()



## Write Ahead Logging (TODO Later)
# Table : Transactions
# [suitable schema to store read/write requests, maybe separate tables]


