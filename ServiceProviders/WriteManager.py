# import tea, coffee whatever





class WriteManager:
    def __init__(self) -> None:
        pass
    
    # functions:

    # send_beat() {regulary sends beat to load balanacer, other managers using separate thread}
    # recv_beat() {from brokers}
    # create_topic(topic_name)
    # size(topic_name, partition_id = None)
    # register_producer(topic_name, parition_id = None) -> success ack
    # enqueue(topic_name, producer_id, message) -> success ack
    #   {use global msg_id, select broker, generate/select paritition_id}
    #   {creating new partitions on existing brokers}
    
    # register_broker(broker_id) -> broker_id
    #   {broker gives its broker_id if it restarts after failure, else supply broker_id}
  
    # list_topics()
    # list_partitions(topic_name)

    ## Write Ahead Logging (TODO Later)
    # General Flow : Receive a request -> log the transaction with enough info to restore
    #                -> interact with broker -> change state of transaction -> sync with other managers
    #                -> commit changes to DB -> delete trasaction_log

	