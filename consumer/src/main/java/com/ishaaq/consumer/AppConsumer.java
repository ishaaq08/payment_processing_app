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
    private Thread workerThread;

    /*
        Assign consumer to the partition payments-0

        This project only uses the partition payments-0. Hence, I believe this is appropriate to be defined in the
        constructor. This is also why assign() is used over subscribe(). It will be interesting to learn about the latter.
        Therefore, given the use of assign(), no logic needs to be implemented that allows multiple partitions to
        be assigned to this consumer.

 */
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
        super.client.assign(new ArrayList<>(Collections.singletonList(paymentsPartition)));
    }

    /**
     * The main method responsible for consuming messages. Executes a while true loop. Within each iteration payments-0
     * is polled (max batch size is 50 - arbitrary). If records have been returned, the batch will be iterated through
     * via a for each loop. parseConsumerRecord and processMessage is called on each ConsumerRecord. The processing time
     * for a batch is also recorded in the table batch_metrics. This is used to determine worst case processing time
     * which is used to adjust max.poll.interval.ms If no records are returned by poll we proceed to the next iteration
     * of the while loop.
     */
    public void consumeMessages() throws InterruptedException {
        System.out.println("--> consuming message from payments-0");

        while (true) {
            ConsumerRecords<String, String> records = super.client.poll(Duration.ofMillis(100));
            List<ConsumerRecord<String, String>> paymentsRecords = records.records(paymentsPartition);

            if (paymentsRecords.isEmpty()) {
                System.out.println("--> no records returned from poll(). Skipping to next iteration.");
                continue;
            } else {
                // pause the partition from the main thread

                // ======== MESSAGE PROCESSING
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
                int batchSize = paymentsRecords.size();
                dbConn.insertBatchMetrics(batchSize, elapsedMs);
                // ======== MESSAGE PROCESSING

                /*
                Commit partition offset for batch of records returned by poll (max batch size is arbitrarily set to 50)
                If processMessage does not yield an error then the message processing was successful. Message will be
                written to db. Transactions will be committed. Now the partition offsets can be committed.

                commitSync(Map<TopicPartition, OffsetAndMetadata>) --> nextOffsets() returns this!
                But, if I did a for loop per partition or topic then I would need to access the specific key from the
                map returned by nextOffsets and pass that to commitSync. This way I will only commit the offsets for
                the partition that has been processed.
                */

                /*
                PROBLEM:
                    The commit needs to occur in the main thread due to the consumer not being thread safe. However, this
                    can't be done in the else statement - after the worker thread is spun to perform processing, the main
                    thread will proceed to this logic and will commit the offsets before the processing has occurred.

                SOLUTION:
                    commitOffsets can't be in the else statement. We should be able to check the status of the
                    worker thread. If it has terminated successfully we then commit the offset. If it is still running
                    we remain paused.

                    > Store the worker thread state and the offsets that it is working on records.nextOffsets().
                        state of the thread can be retrieved by the main thread ConsumerThread.state
                        records.nextOffsets also retrieved by main thread within else statement

                    > For each iteration of the while loop, we should check the status of the worker thread to see if
                    we should commit the offsets and resume. Ideally this check should happen before the poll call is
                    made so that we can invoke resume and then immediately poll more records.
                 */
                super.client.commitSync(records.nextOffsets()); // needs to be done by the main thread
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

    /**
     * The method responsible for processing a single ConsumerRecord. It is called within the for each loop in
     * consumeMessages to process a single record.

     * @param payorId: The ID of the payor
     * @param payeeId: The ID of the payee
     * @param amount: The potential amount to be transferred from the payor to the payee
     * @param transactionId: The unique ID of the transaction
     *
     * @throws RuntimeException if there is an error when processing the message. The error likely comes from any of the
     * methods invoked by dbConn. These methods throw a SQLException which are propagated up the stack as a RuntimeException.
     * Those RuntimeExceptions are caught here.
     */
    private void processMessage(int payorId, int payeeId, int amount, String transactionId) {
        System.out.println("--> parsing consumer record ==");

        boolean doesTransactionExist = dbConn.transactionExists(transactionId);

        if (doesTransactionExist) {
            System.out.println("--> transaction exists");
        } else{
            HashMap<Integer, Integer> balances = dbConn.getPayorAndPayeeBalance(payorId, payeeId);
            int payorCurrentBalance = balances.get(payorId);
            int payeeCurrentBalance = balances.get(payeeId);
            boolean sufficient = payorCurrentBalance >= amount;

            try {
                transferFunds(sufficient, payorId, payorCurrentBalance, payeeId, payeeCurrentBalance, amount, transactionId);
                dbConn.commitTransaction();

            } catch (Exception e) {
                dbConn.rollbackTransaction();
                throw new RuntimeException("Encountered exception when processing message", e);
            }
        }

    }


    /**
     * Method takes 1 of 2 flows depending on the argument passed to the 'sufficient' parameter
     *<p><ol>
     *     <li>'sufficient' = true: Payor has sufficient funds. Update balances of payor and payee in tbl_balance. Insert
     *      transaction into tbl_transactions with an "ACCEPTED" status.</li>
     *      <li> 'sufficient' = false: Payee has insufficient funds. Insert transaction into tbl_transactions with a "DENIED"
     *      * status. No update of balances.</li>
     *</ol></p>

     * @param sufficient: Whether the payor has sufficient funds to complete the transaction
     * @param payorId: The ID of the payor
     * @param payorCurrentBalance: The current balance of the payor
     * @param payeeId: The ID of the payee
     * @param payeeCurrentBalance: The current balance of the payee
     * @param amount: The potential amount to be transferred from the payor to the payee
     * @param transactionId: The unique ID of the transaction
     */
    private void transferFunds(boolean sufficient, int payorId, int payorCurrentBalance, int payeeId,
                               int payeeCurrentBalance, int amount, String transactionId) {
        if (sufficient) {
            System.out.printf("--> payor %s has sufficient funds, updating balances and inserting transaction%n", payorId);
            int payorNewBalance = payorCurrentBalance - amount;
            int payeeNewBalance = payeeCurrentBalance + amount;
            dbConn.performUpdate("tbl_balance", payorNewBalance, payorId);
            dbConn.performUpdate("tbl_balance", payeeNewBalance, payeeId);
            dbConn.insertTransaction(payorId, payeeId, amount, transactionId, "ACCEPTED");
        } else {
            System.out.printf("--> payor %s has insufficient funds, no balance update needed and inserting transaction%n", payorId);
            dbConn.insertTransaction(payorId, payeeId, amount, transactionId, "DENIED");
        }

        }

}
