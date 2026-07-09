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

public class AppConsumer {
    private KafkaConsumer<String, String> client;
    private DatabaseOps dbConn;
    private ObjectMapper objectMapper = new ObjectMapper();;
    private record RecordDetails(int payorId, int payeeId, int amount, String transactionId) {};
    public Thread workerThread;
    private TopicPartition paymentsPartition = new TopicPartition("payments", 0);

    private Map<TopicPartition, OffsetAndMetadata> nextCommittedOffset;
    private int nextBatchSizeToInsert;
    private boolean isPaused;

    public AppConsumer(Map<String, Object> consumerConfigs, Properties databaseConfigs, String databaseUrl) {
        client = new KafkaConsumer<>(consumerConfigs);
            /*
            Assign consumer to the partition payments-0

            This project only uses the partition payments-0. Hence, I believe this is appropriate to be defined in the
            constructor. This is also why assign() is used over subscribe(). It will be interesting to learn about the latter.
            Therefore, given the use of assign(), no logic needs to be implemented that allows multiple partitions to
            be assigned to this consumer.
            */
        client.assign(new ArrayList<>(Collections.singletonList(paymentsPartition)));
        dbConn = new DatabaseOps(databaseConfigs, databaseUrl);
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
            // Check thread status
            if (workerThread == null) {
                System.out.println("--> no task has been assigned to the worker thread");
            } else if (workerThread.getState() == Thread.State.TERMINATED) {
                handleCompletedWorkerThread();
            } else {
                System.out.println("--> worker thread state: " + workerThread.getState());
            }

            ConsumerRecords<String, String> records = client.poll(Duration.ofMillis(100));
            List<ConsumerRecord<String, String>> paymentsRecords = records.records(paymentsPartition);

            // Scenario A: Worker thread is currently processing a batch, thus the partition is paused
            if (isPaused) {
                continue;
            // Scenario B: No records have been returned e.g. consumer fully caught up (zero-consumer lag)
            } else if (paymentsRecords.isEmpty()) {
                System.out.println("--> no records returned from poll(). Skipping to next iteration.");
                continue;
            // Scenario C: New records have been fetched and need to be processed
            } else {
                handleSetupForNewBatch(records, paymentsRecords);

                // Process batch on worker thread
                Task processingTask = new Task(dbConn, records, nextBatchSizeToInsert);
                workerThread = new Thread(processingTask);
                workerThread.start();
                // == End of processing

                    }

//            Thread.sleep(2000);
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

    /**
     * After the worker thread has successfully processed the workload it will have a state of Thread.TERMINATED.
     * 4 operations are then executed:
     * <p>
     *     <ol>
     *         <li>Commit offsets for batch</li>
     *         <li>Resume the partition</li>
     *         <li>Update the isPaused flag to false</li>
     *         <li>Set the worker thread instance variable to null</li>
     *     </ol>
     * </p>
     */
    private void handleCompletedWorkerThread () {
        client.commitSync(nextCommittedOffset);
        client.resume(new ArrayList<>(Collections.singletonList(paymentsPartition)));
        isPaused = false;
        workerThread = null;

        System.out.printf("--> successfully processed a batch of %s records, updated committed offset to %s%n",
                nextBatchSizeToInsert,
                nextCommittedOffset.get(paymentsPartition).offset());

    }

    /**
     * Once a new batch of records has been returned by poll several operations need to occur:
     * <p>
     *     <ol>
     *         <li>Store the committed offset for the batch in the instance field nextCommittedOffset but don't commit!</li>
     *         <li>Store the size of the batch</li>
     *         <li>Pause the partition</li>
     *         <li>Set the isPaused flag to true</li>
     *     </ol>
     * </p>
     *
     * @param records: The batch of records returned in the previous poll() call
     * @param paymentsRecords: A List of the batch of records so the size() method can be used to determine batch size
     */
    private void handleSetupForNewBatch (ConsumerRecords<String, String> records, List<ConsumerRecord<String, String>> paymentsRecords) {
        nextCommittedOffset = records.nextOffsets();
        nextBatchSizeToInsert = paymentsRecords.size();
        client.pause(new ArrayList<>(Collections.singletonList(paymentsPartition)));
        isPaused = true;
    }



}


