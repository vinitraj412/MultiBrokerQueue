# import tea, coffee whatever





class ReadManager:
    def __init__(self) -> None:
        pass
    
    # functions:

    # send_beat() {regulary sends beat to load balanacer, other managers using separate thread}

    # size(topic_name, partition_id = None)
    # register_consumer(topic_name, parition_id = None) -> success ack
    # dequeue(topic_name, consumer_id) -> message
    #   {use messages table to find broker}
    
  
    # list_topics()
    # list_partitions(topic_name)

    ## Write Ahead Logging (TODO Later)
    # General Flow : Receive a request -> log the transaction with enough info to restore
    #                -> interact with broker -> change state of transaction -> sync with other managers
    #                -> commit changes to DB -> delete trasaction_log

	