# Table : Messages (used for finding which broker has a certain message with an offset)
# [topic_name, msg_id(increasing int), broker_id, partition_id]
# msg_id is a global counter for all messages

# Table : Brokers (maps the ip/port of each broker)
# [broker_id, endpoint, last_beat_timestamp]

# Table : Managers (maps the ip/port of other managers)
# [broker_id, endpoint, last_beat_timestamp]

# Table : Partitions (which broker has a particular partition)
# used in round_robin(or random) selection
# [topic_name, partition_id, broker_id]

# Table : Offsets(self explanatory)
# [Consumer_id, topic_name, partition_id(null if subscribed to entire topic), offset]

# Table : Producers
# [producer_id, topic_name, partition_id(null if publishing to entire topic)]

## Write Ahead Logging (TODO Later)
# Table : Transactions
# [suitable schema to store read/write requests, maybe separate tables]


