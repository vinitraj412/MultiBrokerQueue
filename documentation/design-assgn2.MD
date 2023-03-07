# STRUCTURE OF THE APPLICATION(File Structure)

## Load Balancer 
Designed with the help of nginx(used primarily for load balancing), the load balancer is responsible for distributing the traffic between the two servers. It is also responsible for the health check of the servers. The load balancer is configured to check the health of the servers every 5 seconds. If the server is down, the load balancer will not send any traffic to that server. The load balancer is also configured to send 50% of the traffic to server 1 and 50% of the traffic to server 2. The load balancer is configured to listen on port 80. 
All the requests are sent to the load balancer and the load balancer distributes the traffic to the servers.

