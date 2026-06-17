package com.ishaaq.consumer;

import com.ishaaq.app.Builder;
import com.ishaaq.app.PaymentEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.sql.SQLException;
import java.time.Duration;
import java.util.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class AppConsumer extends Builder<KafkaConsumer<String, String>> {
    // A single connection for a consumer, to re-use the same connections when processing records.
    private DatabaseOps dbConn;
    private ObjectMapper objectMapper = new ObjectMapper();;
    private record RecordDetails(int payorId, int payeeId, int amount, String transactionId) {};

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

        ArrayList<Long> runTimes = new ArrayList<>();

        while (true) {
            ConsumerRecords<String, String> records = super.client.poll(Duration.ofMillis(100));

            // For testing ONLY. Message processing time will constrain poll interval.
            System.out.println("========== Processing batch and starting timer ==========");
//            long start = System.nanoTime();

            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("== Processing offset: %s== %n", record.offset());

                // Extract message details
                RecordDetails recordDetails = parseConsumerRecord(record);

                // Process message
                processMessage(
                        recordDetails.payorId,
                        recordDetails.payeeId,
                        recordDetails.amount,
                        recordDetails.transactionId
                );

                TopicPartition recordTopicPartition = new TopicPartition (record.topic(), record.partition());
                OffsetAndMetadata recordOffsetAndMetadata = records.nextOffsets().get(recordTopicPartition);
                long nextOffsetToBe = recordOffsetAndMetadata.offset();
                Optional<Integer> leaderEpoch = recordOffsetAndMetadata.leaderEpoch();

                System.out.printf("--> committed offset: %s%n--> leader epoch: %s%n%n", nextOffsetToBe, leaderEpoch);


            }

            /*
            The batch has finished processing by this point. Logic after this point will only run if the processing is
            successful and no errors were thrown. Therefore, it is appropriate to commit the entire batch here

            My only reservation is that if there is a failure between committing the transaction db and partition offset,
            then once the consumer recovers or there is a rebalancing then the whole batch will be processed again.
            The 'processing' should only involve checking if the transactionId exists which it should. Then its processing
            will be skipped.

            If no records are returned by poll, processing will still occur. No errors will be thrown though. So the commit
            offset logic will be reached. I don't want unnnecessary commit offset requests being made to the broker.
            --> FIX: check if the records > 0, if so execute commit offset logic

            I might have to manually provide topic and partition. Example code uses record within the for loop. This doesn't
            make sense to me.
            --> what offset is returned by nextOffsets().
                offset()
                leaderEpoch()

                expecting the offset to be 50 in each iteration


             */

            // end timer
//            long elapsedMs = (System.nanoTime() - start) / 1_000_000;
//            runTimes.add(elapsedMs);
//
//            if (runTimes.size() == 4) {
//                System.out.printf("Test runs have complete: %s", runTimes);
//                break;
//            }
//            }
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
            System.out.println("--> failure converting JSON string into an object!");
            throw new RuntimeException(e);
        }

        RecordDetails recordDetails = new RecordDetails(
                Integer.parseInt(payloadAsPaymentEvent.payor),
                Integer.parseInt(payloadAsPaymentEvent.payee),
                Integer.parseInt(payloadAsPaymentEvent.amount),
                transactionId
        );

        System.out.printf("--> offset %s: transaction %s : user %s transfers user %s an amount of £%s%n", record.offset(),
                transactionId, recordDetails.payorId, recordDetails.payeeId, recordDetails.amount);

        return recordDetails;
    }

    // Eventually this class will receive the entire payload of the event
    public void processMessage(int payorId, int payeeId, int amount, String transactionId) {
        System.out.println("--> parsing consumer record ==");

        // Check if the transaction already exists in the database
        boolean doesTransactionExist = dbConn.transactionExists(transactionId);

        if (doesTransactionExist) {
            System.out.println("--> transaction exists. Committing partition offset.");
        } else{
            HashMap<Integer, Integer> balances = dbConn.getPayorAndPayeeBalance(payorId, payeeId);
            int payorCurrentBalance = balances.get(payorId);
            int payeeCurrentBalance = balances.get(payeeId);
//            System.out.printf("payor balance: %s, payee balance: %s%n", payorCurrentBalance, payeeCurrentBalance);

            // Check if the payor has sufficient balance i.e. compare to transaction
            boolean sufficient = payorCurrentBalance > amount;
//            System.out.println("--> does the payor have sufficient funds: " + sufficient);

            // == START TRANSACTION == //
            try {
                if (sufficient) {
//                    System.out.println("--> performing balance transfers");
                    int payorNewBalance = payorCurrentBalance - amount;
                    int payeeNewBalance = payeeCurrentBalance + amount;
                    dbConn.performUpdate("tbl_balance", payorNewBalance, payorId);
                    dbConn.performUpdate("tbl_balance", payeeNewBalance, payeeId);

                    dbConn.insertTransaction(payorId, payeeId, amount, transactionId, "ACCEPTED");

                } else {
                    dbConn.insertTransaction(payorId, payeeId, amount, transactionId, "DENIED");
                }

                // == END TRANSACTION == //
                dbConn.commitTransaction();
                // to-add: commit partition offset

                System.out.println("--> message processed: db transaction and partition offset committed.");

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
