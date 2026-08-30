package com.ishaaq.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import com.ishaaq.consumer.ProcessingHelpers.RecordDetails;

public class Task implements Runnable{
    DatabaseOps dbConn;
    ConsumerRecords<String, String> records;
    int nextBatchSizeToInsert;

    Task (DatabaseOps dbConn, ConsumerRecords<String, String> records, int nextBatchSizeToInsert) {
        this.dbConn = dbConn;
        this.records = records;
        this.nextBatchSizeToInsert = nextBatchSizeToInsert;
    }

    @Override
    public void run() {
        long start = System.nanoTime();

        for (ConsumerRecord<String, String> record : records) {
            System.out.printf("== Processing offset: %s== %n", record.offset());

            // Extract message details
            RecordDetails recordDetails = ProcessingHelpers.parseConsumerRecord(record);

            // Process message
            ProcessingHelpers.processMessage(
                    dbConn,
                    recordDetails.payorId(),
                    recordDetails.payeeId(),
                    recordDetails.amount(),
                    recordDetails.transactionId()

            );
        }

        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        dbConn.insertBatchMetrics(nextBatchSizeToInsert, elapsedMs);
    }
}
