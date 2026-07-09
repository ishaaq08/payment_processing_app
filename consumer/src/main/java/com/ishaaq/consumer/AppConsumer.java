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
            handleThreadStatus();

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

            Thread.sleep(2000);
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

    /**
     * This method checks the status of workerThread, from the main thread, at the start of each iteration of the while loop in
     * consumeMessages. It is necessary to monitor the status so we know whether workerThread has completed processing.
     * If the processing is complete handleCompletedWorkerThread is called which commits the offsets and resumes the partition.
     * This must be performed from the main thread as the consumer is not thread-safe.
     * <p>The workerThread is checked against 3 different statuses:</p>
     * <p>
     *     <ol>
     *         <li>null: Occurs on 2 occasions: 1) First iteration of while loop before a workload has been assigned 2)
     *         workerThread successfully finished processing a workload and has been set to null as in handleCompletedWorkerThread.</li>
     *         <li>Thread.TERMINATED: workerThread has finished processing its workload - successfully or unsuccessfully.</li>
     *         <li>Any other state: workerThread is currently processing a workload.</li>
     *     </ol>
     * </p>
     */
    private void handleThreadStatus() {
        if (workerThread == null) {
            System.out.println("--> no task has been assigned to the worker thread");
        } else if (workerThread.getState() == Thread.State.TERMINATED) {
            handleCompletedWorkerThread();
        } else {
            System.out.println("--> worker thread state: " + workerThread.getState());
        }
    }

}


