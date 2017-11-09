# Kafka Streams Microservice Examples

Here is a small microservice ecosystem built with Kafka Streams. 


It centers around an Orders Service which provides a REST interface to POST and GET Orders. Posting an Order creates an event in Kafka. This is picked up by three different validation engines (Fraud Service, Inventory Service, Order Details Service) which validate the order in parallel, emitting a PASS or FAIL based on whether each validation succeeds. The result of each validation is pushed through a separate topic, Order Validations, so that we retain the ‘single writer’ status of the Orders Service —> Orders Topic. The results of the various validation checks are aggregated back in the Order Service (Validation Aggregator) which then moves the order to a Validated or Failed state, based on the combined result. 

To allow users to GET any order, the Orders Service creates a queryable materialized view (embedded inside the Orders Service), using a state store in each instance of the service, so any Order can be requested historically. Note also that the Orders Service can be scaled out over a number of nodes, so GET requests must be routed to the correct node to get a certain key. This is handled automatically using the Interactive Queries functionality in Kafka Streams. 

The Orders Service also includes a blocking HTTP GET so that clients can read their own writes. In this way we bridge the synchronous, blocking paradigm of a Restful interface with the asynchronous, non-blocking processing performed server-side.


# Running the examples:
* Requires Java 1.8
* mvn install -Dmaven.test.skip=true
