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
