# Distributed Logging Queue
# Design Choices Documentation 

## Part A (Distributed Logging Queue)

### Implementation

Our design for the Distributed Logging Queue (aka Connectify) includes two packages:  
- `TopicQueue`: A lower level queue maintained for every topic which stores all the data corresponding to the topic, namely the messages which are stored as a list and a self lock for synchronization.
- `Distributed Queue`: A top level package which helps us interact with the lower level topic queues and also stores the user data such as the consumer and producer IDs corresponding to every topic as a dictionary mapped from topic names to the IDs which are in a set.
 
The Distributed Queue package is an umbrella module which helps us interact with the system while the Topic Queues store the topic data which is accessed via the top level package. A topic dictionary is maintained with each topic name mapped to their self locks. We have also created 3 locks for the Distributed queue:
- For the consumer ID dictionary access
- For the producer ID dictionary access
- For the topic dictionary access.

The API wrapper used to interact with the endpoints was implemented using Flask. Postman was integrated into the Flask interface to send and recieve prompts in the JSON format while testing.


### Challenges

A challenge in the implementation was ensuring concurrency, which was done with the aid of locks for safety. We also had to optimize the locking process to optimize the running time of various retrieval and add methods, and hence we used multiple locks for different data structures, to increase the granularity of the program. 
With a large number of locks each independently locking small amounts of data, the lock contention is reduced, which decreases the waiting time of the program compared to the case where a smaller number of locks is used to protect larger amounts of data, because this increases the likelihood of the lock stopping an unrelated process from running concurrently.   

The generation of unique and random consumer and producer IDs across concurrently executing threads was also a difficulty that we came across, but it was solved by the usage of the inbuilt module called uuid (universally unique identifier), which can generate random 128-bit unique IDs which consist of hexadecimal digits. 


### Testing 

To test the implementation, we conducted a series of tests. 

The tests included:
- Checking that the queue is able to handle multiple log messages at the same time.
- Ensuring that the queue is able to handle high concurrency without data corruption or race conditions.
- Testing the queue's behavior when it is under heavy load.


## Part B (Persistence in the Queue)

In this part, we had to add persistence to the program, (i.e) recovery insurance even after server crashes or restarts. This was done by adding a persistent storage layer into the interface which provides an alternative location for storing the messages instead of memory, which would result in data loss in case of a crash. 

### Implementation

When an additional storage layer (database) is added into the program, our initial queue design needed some modifications. First of all, the topic queues which were initially stored seperately for each topic in-memory were merged into one table inside the database as it was more efficient than making a seperate table for each topic queue.  We maintained an offset for every consumer which would inform us the next message that the specific consumer has to read and also if the consumer has reached the end of the message queue. 

As the database has it's own access commands, the need of a high-level package to access the queue contents also becomes redundant, and hence we removed the Topic Queue package from our implementation. 

Also, as we are already using a database to store the messages and the other relevant metadata and user data, it eliminates the need of locks to ensure synchronization and hence the instances of lock usages were removed too. 

For implementing the database, a PostgreSQL database was set up and an Object Relational Manager (ORM) was used to create a bridge between our already existing Objected Orient Program which consists of class objects and these are stored in the Relational Database using the ORM, which is Flask-SQLAlchemy in our case. 

Flask-SQLAlchemy provides persistence patterns designed for efficient and high-performing database access and hence was our first choice as an ORM. In our case, we used the ORM to create four additional classes (created in Models.py) to get, add and update specific data items (such as offsets, topic size, etc.) and these classes were used in our main program for accessing the database.  

Firstly, we imported the database access classes in our main program. Next, all the instances of access to data items in our original program were modified by using the newly created methods of our database access classes. New consumers and producers are also added via methods of these four new classes. The API Wrapper was modified to incorporate the usage of SQLAlchemy as an ORM. 

#### The details of the four model classes are :

##### Topic_Names
    	contains: topic_name {supports CreateTopic, ListTopic}

##### TopicMessages
	contains: message_id, topic_name, producer_id, message {supports enqueue, dequeue, size}

##### TopicOffsets
	contains: consumer_id, topic_name, offset {supports RegisterConsumer, used in deqeue, size}

##### TopicProducer
	contains: topic_name, producer_id {used for checking if producer exists for given topic}


### Challenges

One challenge that we encountered during the implementation process was ensuring that the script could handle large volumes of logs without causing performance issues. This required some optimization and testing to find the right balance. 

Another hurdle was ensuring that the script could handle different types of logs and insert them into the correct columns in the database. This required careful design of the database schema and testing to ensure that all logs were properly formatted and inserted. 

A difficulty which we faced during implementation was that a lot of redundant code was being written because the same database access methods were being called, which are quite lengthy in comparison to normal data structure access, hence we created four additonal classes for database access. 


### Testing 

To test the implementation, We first set up a test PostgreSQL database and tested the Python script's ability to connect and insert logs into the database. We had also tested the script's ability to handle large volumes of logs, and checked for any performance issues.


## Part C (Client Library Implementation)

### Implementation

In the earlier parts, we were using the postman tool to access our distributed queue API which was created in Flask, which in turn used our Queue package for various functionalities such as retrieving queue logs, registering consumers and producers and many other methods. In this part, we had to implement client libraries for both the consumers and the producers such that the need of a third-party API is eliminated and the libraries can be used directly to access our API. 

The scripts for the Producer and Consumer libraries were written in python. Because we had to implement the Application Programming Interface from scratch, we had to incorporate various checks which included HTTP and connection errors. 


### Challenges

One of the major challenges that was faced while implementing this part was familiarising ourselves and finding the equivalent python functions for the flask method calls so that we could use them appropriately in our SDK. 

Another challenge we faced was the SDK always has non-blocking calls so the retry routine of the producer has to be written from the user side. If addding a message to the queue fails, then the onus of retrying belongs to the user of the SDK. As we didn't want to make sdk blocking.

### Testing and Hyperparameters
The Hyperparameters and tests considered for this part were similar to that of part A. 