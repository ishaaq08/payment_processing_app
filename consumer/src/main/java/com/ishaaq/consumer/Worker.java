package com.ishaaq.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.protocol.types.Field;

public class Worker {
    private Thread workerThread;
    public ThreadExceptionData workerThreadExceptionData = new ThreadExceptionData();

    Worker (DatabaseOps dbConn, ConsumerRecords<String, String> records, int nextBatchSizeToInsert) {
        setWorkerThread(dbConn, records, nextBatchSizeToInsert);
        workerThread.setUncaughtExceptionHandler(new WorkerExceptionHandler(workerThreadExceptionData));
    }

    // Getter(s)
    public Thread getWorkerThread() {
        return workerThread;
    }

    // Setter(s)

    // Method
    private void setWorkerThread(DatabaseOps dbConn, ConsumerRecords<String, String> records, int nextBatchSizeToInsert) {
        Task processingTask = new Task(dbConn, records, nextBatchSizeToInsert);
        workerThread = new Thread(processingTask);
    }

    public void runWorkerThread() {
        workerThread.start();
    }

    public Thread.State getWorkerThreadState() {
        return workerThread.getState();
    }

}
