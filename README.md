# Motivation for Project
A hobby project with the intention of deepening my understanding of Kafka and the implementation of distributed systems. With Kafka being written in Java (and Scala) coupled with my minimal exposure to statically-typed languages, I thought it was appropriate to program this project using Java. The shift from dynamic to static typing greatly improved my ability to understand what an operation does and pay careful intention to its inputs and outputs. I also noticed less bugs to be present in my code due to the planning that statically-typed languages require. 

# About Project
A payment processing pipeline leveraging Kafka.

## High-Level Workflow
- Producer uploads payment event to topic "payments".
- Consumer retrieves messages from "payments"
  - a) If the payor has sufficient funds write to database and populate the field [Status] with "Accepted"
  - b) If the payor has insufficient funds write to database and populate the field [Status] with "Denied"

# Future Work
## Deploy to Cloud
Host entire workflow on Azure.

## More Complex Workflow
The current workflow can be referred to as "Scope 1". I wish to build on Scope 1 to develop Scope 2. My current thinking is:

- Producer A will write a payment to the payments_created topic
  - Key: transaction_id
  - Value: amount, payor_card_number, payee_card_number, status (= PENDING), timestamp
- Service A acting as a consumer will get this record from payments_created.
- Service A will insert a record for the transaction into the database. 
- Service A will then act as a producer and write this record to payments_validation_requested.
  - If the write is successful it will update the status of the record in the DB from PENDING to SENT FOR VALIDATION
  - If the write failed it will update the status of the record in the DB from PENDING to ERROR
- Service B (the issuing bank) will act as a consumer and read the record from payments_validation_requested.
- Service B will validate the payor has sufficient details to complete this transaction.
  - ℹ️To perform this operation Service B should have access to a database storing information relating to bank balances of it’s customers
- Service B will write the result of the validation to payments_validated
- Consumer A will read the record from payments_validated and will update the status of the transaction to either ACCEPTED or DENIED

# Kafka Learnings
### Offset
- The position of a record in a partition.
- 2 notions:
    - **Position**: I like to think of this as the current position of the consumer. More specifically, it is the offset in the partition that the consumer will consume from when retrieving records during the next poll() call. This value automatically advances to the offset after the last record returned by the poll. For example, if the last poll returned offsets 10 - 20, the position is now 21. This can be manually overridden using the **seek()** method.
    - **Committed Offset**: per consumer group, it is the partition offset that a subscribed consumer will consume from, after recovery/restart or rebalancing. Given **n** represents the last record that has been processed securely, the committed offset will be **n+1**.
        - The Java client for Kafka states ["The committed position is the last offset that has been stored securely"](https://javadoc.io/doc/org.apache.kafka/kafka-clients/latest/org/apache/kafka/clients/consumer/KafkaConsumer.html), however I believe this is misleading. The definition given in this article by [Confluent](https://www.confluent.io/blog/guide-to-consumer-offsets/) is clearer: "It's important to note that the committed offset refers to the next offset the consumer intends to read, not the offset of the last successfully processed message". This is supported by the fact that when manually committing offsets, you update the committed offset to n+1 not n. See the previous link for a code example.
       - The committed offset is stored in an internal topic called **__consumer_offsets** and so is durable.
- **Position vs Committed Offset**: This confused me at first! What helped me to understand this was looking at the two from the perspective of durability. The committed offset is stored on an internal topic on a broker but the position isn't. Hence why the former can be used if a consumer crashes and restarts or rebalancing occurs i.e. the value is written somewhere. This may not be correct but I like to think of position as existing at the application layer - if the application crashes the position does not persist as it isn't written anywhere but the committed offset/position does persist.
