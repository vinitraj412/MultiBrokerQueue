from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()

# Table : Brokers (maps the ip/port of each broker)
# [broker_id, endpoint, last_beat_timestamp]
class BrokerMetadata(db.Model):
    pass

# Table : Managers (maps the ip/port of other managers)
# [broker_id, endpoint, last_beat_timestamp]
class ManagerMetadata(db.Model):
    pass

# Table : Partitions (which broker has a particular partition)
# used in round_robin(or random) selection
# [topic_name, partition_id, broker_id]
class PartitionMetadata(db.Model):
    pass

# Table : Messages (used for finding which broker has a certain message with an offset)
# [topic_name, id(increasing int), broker_id, partition_id]
# id is a global counter for all messages

class ManagerMessageView(db.Model):
    __tablename__ = 'ManagerMessageView'
    topic_name = db.Column(db.String(),)
    partition_id = db.Coumn(db.String(),db.ForeignKey('PartitionMetadata.partition_id'))
    broker_id = db.Coumn(db.String(),db.ForeignKey('BrokerMetadata.broker_id'))
    id = db.Column(db.Integer, primary_key=True)

    def __init__(self,topic_name,partition_id,broker_id):
        self.broker_id=broker_id
        self.topic_name=topic_name
        self.partition_id=partition_id

    @staticmethod
    def getBrokerID(targetTopic, targetPartitionId,targetOffset):
        return ManagerMessageView.query.filter_by(topic_name=targetTopic, partition_id = targetPartitionId)[targetOffset].broker_id
    
    @staticmethod
    def addMessageMetadata(topic_name,partition_id, broker_id):
        ## check topic name????
        if not BrokerMetadata.checkBroker(): ## check if broker still up
            raise Exception("Broker down")
        
        message_entry=ManagerMessageView(topic_name,partition_id,broker_id)
        db.session.add(message_entry)
        db.session.commit()


# Table : Offsets(self explanatory)  
# [Consumer_id, topic_name, partition_id(null if subscribed to entire topic), offset]

# Table : Producers
# [producer_id, topic_name, partition_id(null if publishing to entire topic)]

## Write Ahead Logging (TODO Later)
# Table : Transactions
# [suitable schema to store read/write requests, maybe separate tables]


