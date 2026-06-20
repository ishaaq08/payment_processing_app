package com.ishaaq.consumer;

import com.ishaaq.app.Builder;
import com.ishaaq.app.PaymentEvent;
import com.ishaaq.app.Topic;
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
    private DatabaseOps dbConn;
    private ObjectMapper objectMapper = new ObjectMapper();;
    private record RecordDetails(int payorId, int payeeId, int amount, String transactionId) {};
    private TopicPartition paymentsPartition = new TopicPartition("payments", 0);

    public AppConsumer(Map<String, Object> consumerConfigs, Properties databaseConfigs, String databaseUrl) {
        super(consumerConfigs);
        dbConn = new DatabaseOps(databaseConfigs, databaseUrl);
    }

    // Abstract method override
    @Override
    public void createClient () {
        System.out.println("--> setting consumer client to super.configs ");
        super.client = new KafkaConsumer<>(super.configs);

        /*
        Assign consumer to the partition payments-0
            This project only uses the partition payments-0. Hence, I believe this is appropriate to be defined in the
             constructor. This is also why assign() is used over subscribe(). It will be interesting to learn about the latter.
             Therefore, given the use of assign(), no logic needs to be implemented that allows multiple partitions to
              be assigned to this consumer.

         */
        super.client.assign(new ArrayList<>(Collections.singletonList(paymentsPartition)));
    }

    public void consumeMessages() throws InterruptedException {
        System.out.println("--> consuming message from payments-0");

        while (true) {
            ConsumerRecords<String, String> records = super.client.poll(Duration.ofMillis(100));

            List<ConsumerRecord<String, String>> paymentsRecords = records.records(paymentsPartition);
            int batchSize = paymentsRecords.size();

            if (paymentsRecords.isEmpty()) {
                // No records have been returned for whatever reason > skip to the next iteration of the while loop
                System.out.println("--> no records returned from poll(). Skipping to next iteration.");
                continue;
            } else {
                System.out.println("========== Processing batch and starting timer ==========");

                long start = System.nanoTime();
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
                }
                long elapsedMs = (System.nanoTime() - start) / 1_000_000;
                dbConn.insertBatchMetrics(batchSize, elapsedMs);

                /*
                Commit partition offset for batch of records returned by poll (max batch size is arbitrarily set to 50)
                If processMessage does not yield an error then the message processing was successful. Message will be
                written to db. Transactions will be committed. Now the partition offsets can be committed.

                commitSync(Map<TopicPartition, OffsetAndMetadata>) --> nextOffsets() returns this!
                But, if I did a for loop per partition or topic then I would need to access the specific key from the
                map returned by nextOffsets and pass that to commitSync. This way I will only commit the offsets for
                the partition that has been processed.
                */
                super.client.commitSync(records.nextOffsets());
                System.out.printf("--> successfully processed a batch of %s records, updated committed offset to %s%n",
                        batchSize,
                        records.nextOffsets().get(paymentsPartition).offset());

                }

            Thread.sleep(2000);
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

        boolean doesTransactionExist = dbConn.transactionExists(transactionId);

        if (doesTransactionExist) {
            System.out.println("--> transaction exists. Committing partition offset.");
        } else{
            HashMap<Integer, Integer> balances = dbConn.getPayorAndPayeeBalance(payorId, payeeId);
            int payorCurrentBalance = balances.get(payorId);
            int payeeCurrentBalance = balances.get(payeeId);
//            System.out.printf("payor balance: %s, payee balance: %s%n", payorCurrentBalance, payeeCurrentBalance);

            boolean sufficient = payorCurrentBalance > amount;
//            System.out.println("--> does the payor have sufficient funds: " + sufficient);

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

                dbConn.commitTransaction();

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
