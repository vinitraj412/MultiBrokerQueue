from flask_sqlalchemy import SQLAlchemy
from datetime import datetime
# from sortedcollections import OrderedSet

db = SQLAlchemy()

# Table : Brokers (maps the ip/port of each broker)
# Here's where we store the details about the brokers
# [broker_id, endpoint, last_beat_timestamp]

# service registry
class BrokerMetadata(db.Model):
    __tablename__ = 'BrokerMetadata'

    broker_id = db.Column(db.Integer(), primary_key=True)
    endpoint = db.Column(db.String())
    last_beat_timestamp = db.Column(db.DateTime, default=datetime.utcnow)
    status = db.Column(db.Boolean, default=True)

    def __init__(self, endpoint):
        self.endpoint = endpoint

    def __repr__(self) -> str:
        return "<Broker %r>" % self.endpoint

    @staticmethod
    def updateTimeStamp(broker_id):
        broker = BrokerMetadata.query.filter_by(broker_id=broker_id).first()
        broker.last_beat_timestamp = datetime.utcnow()
        db.session.commit()

    @staticmethod
    def updateIP(broker_id, endpoint):
        broker = BrokerMetadata.query.filter_by(broker_id=broker_id).first()
        broker.endpoint = endpoint
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
    def checkBroker(broker_id) -> bool:
        broker = BrokerMetadata.query.filter_by(broker_id=broker_id).first()
        return BrokerMetadata.isActiveBroker(broker)
    # if ((datetime.utcnow() - broker.last_beat_timestamp).microseconds < 300000) else False

    @staticmethod
    def get_active_brokers() -> list:
        return [broker.broker_id for broker in BrokerMetadata.query.all() if BrokerMetadata.isActiveBroker(broker)]
        # TODO (b-a).seconds * 1000000 + (b-a).microseconds
        # if (datetime.utcnow() - broker.last_beat_timestamp).microseconds < 300000]

    @staticmethod
    def isActiveBroker(broker) -> bool:
        return (datetime.utcnow() - broker.last_beat_timestamp).total_seconds() < 0.3

    @staticmethod
    def getBrokerEndpoint(broker_id: int) -> str:
        broker = BrokerMetadata.query.filter_by(broker_id=broker_id).first()
        return broker.endpoint

    @staticmethod
    def getBrokerId(endpoint) -> int:
        broker = BrokerMetadata.query.filter_by(endpoint=endpoint).first()
        return broker.broker_id if broker else -1

# Table : Managers (maps the ip/port of other managers)
# We are supposed to store the details of the manager?
# This might be used for sending the heartbeat
# [broker_id, endpoint, last_beat_timestamp]
# This might be done using
# class ManagerMetadata(db.Model):
#     pass

# Table : Partitions (which broker has a particular partition)
# Maps the partition (topic_name, partition_id) to the broker_id
# used in round_robin(or random) selection
# [topic_name, partition_id, broker_id]


class PartitionMetadata(db.Model):
    __tablename__ = 'PartitionMetadata'

    id = db.Column(db.Integer(), primary_key=True)
    topic_name = db.Column(db.String())
    partition_id = db.Column(db.Integer())
    broker_id = db.Column(db.Integer(), db.ForeignKey('BrokerMetadata.broker_id'))
    size = db.Column(db.Integer(), default=0)

    __table_args__ = (
        # this can be db.PrimaryKeyConstraint if you want it to be a primary key
        db.UniqueConstraint('partition_id', 'topic_name'),
      )

    def __init__(self, topic_name, broker_id):
        self.topic_name = topic_name
        self.broker_id = broker_id
        # find max partition_id for this topic and add 1
        max_part_id = db.session.query(db.func.max(
            PartitionMetadata.partition_id)).filter_by(topic_name=topic_name).scalar()
        if max_part_id is None:
            self.partition_id = 0
        else:
            self.partition_id = max_part_id + 1

    @staticmethod
    def createPartition(topic_name, broker_id):
        entry = PartitionMetadata(topic_name, broker_id)
        try:
            db.session.add(entry)
            db.session.commit()
        except:
            db.session.rollback()
            return -1
        partition_id = entry.partition_id
        return partition_id

    # @staticmethod
    # def exist(topic_name, partition_offset):
    #     if PartitionMetadata.query.filter_by(topic_name=topic_name).count() > partition_offset:
    #         return True
    #     else:
    #         return False

    @staticmethod
    def listTopics():
        print(PartitionMetadata.query.all())
        query = [partition.topic_name for partition in PartitionMetadata.query.all()]
        return list(set(query))

    @staticmethod
    def listPartition_IDs(topic_name):
        query = [entry.partition_id for entry in PartitionMetadata.query.filter_by(topic_name=topic_name).all() if BrokerMetadata.checkBroker(entry.broker_id)]
        return sorted(list(set(query)))

    @staticmethod
    def getPartition_Metadata(topic_name, partition_id):
        import sys
        print(f" AAAAAAAAAAAAAAAAAAAAA {topic_name} {partition_id}", file=sys.stderr)
        return PartitionMetadata.query.filter_by(topic_name=topic_name, partition_id=partition_id).first().id
    
    @staticmethod
    def increaseSize(topic_name, partition_id):
        partition = PartitionMetadata.query.filter_by(topic_name=topic_name, partition_id=partition_id).first()
        partition.size += 1
        db.session.commit()

    @staticmethod
    def getSize(topic_name, partition_id):
        return PartitionMetadata.query.filter_by(topic_name=topic_name, partition_id=partition_id).first().size
    
    @staticmethod
    def getBrokerID(topic_name, partition_id):
        return PartitionMetadata.query.filter_by(topic_name=topic_name, partition_id=partition_id).first().broker_id

    @staticmethod
    def checkPartition(topic_name, partition_id):
        return PartitionMetadata.query.filter_by(topic_name=topic_name, partition_id=partition_id).count() > 0

# Table : Messages (used for finding which broker has a certain message with an offset)
# [topic_name, id(increasing int), broker_id, partition_id]
# id is a global counter for all messages
# This stores the details for all the messages
# So all the messages regardless of the topic or partition are stored here
# class ManagerMessageView(db.Model):
#     __tablename__ = 'ManagerMessageView'
#     topic_name = db.Column(db.String(),db.ForeignKey('PartitionMetadata.partition_id'))
#     partition_id = db.Column(db.Integer(),db.ForeignKey('PartitionMetadata.partition_id')) # can this be NULL?
#     id = db.Column(db.Integer, primary_key=True)

#     def __init__(self,topic_name,partition_id,broker_id):
#         self.broker_id=broker_id
#         self.topic_name=topic_name
#         self.partition_id=partition_id

#     @staticmethod
#     def getBrokerID(targetTopic, targetPartitionId, targetOffset):
#         return ManagerMessageView.query.filter_by(topic_name=targetTopic, partition_id = targetPartitionId)[targetOffset].broker_id

#     def getBrokerIDGlobalOffset(targetTopic, targetOffset):
#         # How do we get target offset here? Need randomization!
#         entry=ManagerMessageView.query.filter_by(topic_name=targetTopic)[targetOffset]
#         return entry.broker_id,entry.partition_id

#     @staticmethod
#     def addMessageMetadata(topic_name,partition_id, broker_id):
#         ## check topic name????

#         ## no need to check since foreign key reference
#         # if not BrokerMetadata.checkBroker(): ## check if broker still up
#          #     raise Exception("Broker down")

#         message_entry=ManagerMessageView(topic_name,partition_id,broker_id)
#         try:
#             db.session.add(message_entry)
#             db.session.commit()
#         except:
#             db.session.rollback()

#     @staticmethod
#     def getLeastMessageBroker(topic_name):
#         brokers = set([b.broker_id for b in BrokerMetadata.query.all()])

#         query = ManagerMessageView.query.filter_by(topic_name=topic_name).all()
#         broker_message_load = {b:0 for b in brokers}
#         for q in query:
#             broker_message_load[q] += 1
#         if(len(q) > 0):
#             return min(broker_message_load, key = broker_message_load.get)

#         return -1


# Table : Offsets(self explanatory)
# [Consumer_id, topic_name, partition_id(null if subscribed to entire topic), offset]
class ConsumerMetadata(db.Model):
    __tablename__ = 'ConsumerMetadata'
    consumer_id = db.Column(db.String(), primary_key=True)
    partition_metadata = db.Column(db.Integer(), primary_key=True)
    offset = db.Column(db.Integer())  # will be

    def __init__(self, consumer, topic_name, partition_id, offset):
        self.consumer_id = consumer
        self.partition_metadata = PartitionMetadata.getPartition_Metadata(topic_name, partition_id)
        self.offset = offset

    @staticmethod
    def registerConsumer(consumer_id, topic_name, partition_id):
        entry = ConsumerMetadata(consumer_id, topic_name, partition_id, 0)
        db.session.add(entry)
        db.session.commit()

    @staticmethod
    def getOffset(topic_name, consumer_id, partition_id):
        part_metadata = PartitionMetadata.getPartition_Metadata(topic_name, partition_id)
        obj= ConsumerMetadata.query.filter_by(consumer_id=consumer_id, partition_metadata=part_metadata).first()
        if obj is None:
            return 0
        return obj.offset

    @staticmethod
    def incrementOffset(consumer_id, topic_name, partition_id):
        import sys
        print(f" BBBBBBBBBBBBBBB {consumer_id} {topic_name} {partition_id}", file=sys.stderr)
        part_metadata = PartitionMetadata.getPartition_Metadata(topic_name, partition_id)
        
        if not ConsumerMetadata.checkConsumer(consumer_id, topic_name, partition_id):
            ConsumerMetadata.registerConsumer(consumer_id, topic_name, partition_id)
            
        entry = ConsumerMetadata.query.filter_by(consumer_id=consumer_id, partition_metadata=part_metadata).first()
        entry.offset += 1
        db.session.commit()

    @staticmethod
    def getConsumerCount(topic_name, partition_id):
        part_metadata = PartitionMetadata.getPartition_Metadata(topic_name, partition_id)
        return ConsumerMetadata.query.filter_by(partition_metadata=part_metadata).count()

    # @staticmethod
    # def updateConsumerPartition(consumer_id, new_partition_metadata):
    #     entry=ConsumerMetadata.query.filter_by(consumer_id=consumer_id).first()
    #     entry.partition_metadata=new_partition_metadata
    #     db.session.commit()
    
    @staticmethod
    def checkConsumer(consumer_id, topic_name, partition_id):
        part_metadata = PartitionMetadata.getPartition_Metadata(topic_name, partition_id)
        return ConsumerMetadata.query.filter_by(consumer_id=consumer_id, partition_metadata=part_metadata).count() > 0



# Table : Producers
# [producer_id, topic_name, partition_id(null if publishing to entire topic)]
class ProducerMetadata(db.Model):
    __tablename__ = 'ProducerMetadata'
    producer_id = db.Column(db.String(), primary_key=True)
    topic_name = db.Column(db.String())

    def __init__(self, producer_id, topic_name):
        self.producer_id = producer_id
        self.topic_name = topic_name

    @staticmethod
    def registerProducer(producer_id, topic_name):
        entry = ProducerMetadata(producer_id, topic_name)
        db.session.add(entry)
        db.session.commit()
    
    @staticmethod
    def topic_registered(producer_id, topic_name):
        return ProducerMetadata.query.filter_by(producer_id=producer_id, topic_name=topic_name).count() > 0
    
    @staticmethod
    def getTopic(producer_id):
        return ProducerMetadata.query.filter_by(producer_id=producer_id).first().topic_name
