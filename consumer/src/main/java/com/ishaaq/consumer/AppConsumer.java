package com.ishaaq.consumer;

import com.ishaaq.app.Builder;
import com.ishaaq.app.PaymentEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class AppConsumer extends Builder<KafkaConsumer<String, String>> {
    // Required to pass into process().
    // A single connection for a consumer, to re-use the same connections when processing records.
    private DatabaseOps dbConn;
    private ObjectMapper objectMapper;
    private record RecordDetails(int payorId, int payeeId, int amount, String transactionId) {};

    // Constructor
    public AppConsumer(Map<String, Object> consumerConfigs, Properties databaseConfigs, String databaseUrl) {
        super(consumerConfigs);
        dbConn = new DatabaseOps(databaseConfigs, databaseUrl);
    }

    // Abstract method override
    @Override
    public void createClient () {
        System.out.println("--> setting consumer client to super.configs ");
        super.client = new KafkaConsumer<>(super.configs);
    }

    public void consumeMessages() {
        // How are partitions assigned in production?
        // For testing, we will just do manual assignment of partitions
        System.out.println("== Consuming message from partition ==");
        System.out.println("--> consuming message from payments-0");
        TopicPartition partition0 = new TopicPartition("payments", 0);
        ArrayList<TopicPartition> allPartitions = new ArrayList<>();
        allPartitions.add(partition0);
        super.client.assign(allPartitions);


        while (true) {
            ConsumerRecords<String, String> records = super.client.poll(Duration.ofMillis(100));
            // Store and output the number of records returned - should be 50 - assert?

            // start timer
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("Processing offset: %s%n", record.offset());

                // Extract message details
                RecordDetails recordDetails = parseConsumerRecord(record);

                // Process message
                processMessage(
                        recordDetails.payorId,
                        recordDetails.payeeId,
                        recordDetails.amount,
                        recordDetails.transactionId
                );


            }

            // end timer
            // print runtime
        }

    }

    /**
     * Parse a ConsumerRecord and return the key and payload. jackson is used convert the JSON string from the payload
     * into a PaymentEvent object. The fields from PaymentEvent, now storing the payload details, are then passed in
     * as values to the returned HashMap.

     * @param record: The ConsumerRecord to be parsed
     *
     * @return recordDetails: HashMap containing the keys: payorId, payeeId, amount, transactionId. All values are
     * int apart from the value for transactionId which is a String.
     */
    private RecordDetails parseConsumerRecord(ConsumerRecord<String, String> record) {
        String transactionId = record.key();
        String payloadJson = record.value();
        PaymentEvent payloadAsPaymentEvent;

        try{
            payloadAsPaymentEvent = objectMapper.readValue(payloadJson, PaymentEvent.class);
        } catch (JsonProcessingException e) {
            System.out.println("--> failure converting object into a JSON string via the Jackson package!");
            throw new RuntimeException(e);
        }

        return new RecordDetails(
                Integer.parseInt(payloadAsPaymentEvent.payor),
                Integer.parseInt(payloadAsPaymentEvent.payee),
                Integer.parseInt(payloadAsPaymentEvent.amount),
                transactionId
        );
    }

    // Eventually this class will receive the entire payload of the event
    public void processMessage(int payorId, int payeeId, int amount, String transactionId) {
        System.out.println("== Processing partition message ==");

        // Check if the transaction already exists in the database
        boolean doesTransactionExist = dbConn.transactionExists(transactionId);

        if (doesTransactionExist) {
            System.out.println("ℹ️ Transaction exists. Committing partition offset.");
        } else{
            HashMap<Integer, Integer> balances = dbConn.getPayorAndPayeeBalance(payorId, payeeId);
            int payorCurrentBalance = balances.get(payorId);
            int payeeCurrentBalance = balances.get(payeeId);
            System.out.printf("payor balance: %s, payee balance: %s%n", payorCurrentBalance, payeeCurrentBalance);

            // Check if the payor has sufficient balance i.e. compare to transaction
            boolean sufficient = payorCurrentBalance > amount;
            System.out.println("--> does the payor have sufficient funds: " + sufficient);

            // == START TRANSACTION == //
            try {
                if (sufficient) {
                    // User has sufficient funds
                    // Update balances
                    System.out.println("--> performing balance transfers");
                    int payorNewBalance = payorCurrentBalance - amount;
                    int payeeNewBalance = payeeCurrentBalance + amount;
                    dbConn.performUpdate("tbl_balance", payorNewBalance, payorId);
                    dbConn.performUpdate("tbl_balance", payeeNewBalance, payeeId);

                    // Insert transaction with "ACCEPTED" status
                    dbConn.insertTransaction(payorId, payeeId, amount, transactionId, "ACCEPTED");

                } else {
                    // User has insufficient funds and no balance updates required
                    // Insert transaction with "DENIED" status
                    dbConn.insertTransaction(payorId, payeeId, amount, transactionId, "DENIED");
                }

                // == END TRANSACTION == //
                dbConn.commitTransaction();
                // to-add: commit partition offset

                System.out.println("✅ Successfully processed messaged: db transaction and partition offset committed.");

            } catch (Exception e) {
                // The method performUpdate() and insertTransaction() will catch a SQLException. They will throw a RuntimeException
                // ... which will be caught here. We can't explicitly specify a SQLException in this catch block because none of
                // ... the above logic throws it
                dbConn.rollbackTransaction();
                throw new RuntimeException("Encountered exception when processing message", e);
            }
        }

    }
}
