package com.ishaaq.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.protocol.types.Field;

public class Worker {
    private Thread workerThread = null;
    public ThreadExceptionData workerThreadExceptionData = new ThreadExceptionData();

    Worker (DatabaseOps dbConn, ConsumerRecords<String, String> records, int nextBatchSizeToInsert) {
        setWorkerThread(dbConn, records, nextBatchSizeToInsert);
    }

    // Getter(s)
    public Thread getWorkerThread() {
        return workerThread;
    }

    // Setter(s)
    public void setWorkerThread(DatabaseOps dbConn, ConsumerRecords<String, String> records, int nextBatchSizeToInsert) {
        Task processingTask = new Task(dbConn, records, nextBatchSizeToInsert);
        workerThread = new Thread(processingTask);
    }

    // Method
    public void runWorkerThread() {
        workerThread.start();
    }

    public Thread.State getWorkerThreadState() {
        return workerThread.getState();
    }

}
