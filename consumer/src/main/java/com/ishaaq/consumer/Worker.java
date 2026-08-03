package com.ishaaq.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.protocol.types.Field;

public class Worker {
    private Thread workerThread = null;
    public ThreadExceptionData workerThreadExceptionData = new ThreadExceptionData();

    // Getter(s)
    public Thread getWorkerThread() {
        return workerThread;
    }

    // Setter(s)

    // Method
    public void setWorkerThread(DatabaseOps dbConn, ConsumerRecords<String, String> records, int nextBatchSizeToInsert) {
        Task processingTask = new Task(dbConn, records, nextBatchSizeToInsert);
        workerThread = new Thread(processingTask);
        workerThread.setUncaughtExceptionHandler(new WorkerExceptionHandler(workerThreadExceptionData));
    }

    public void runWorkerThread() {
        workerThread.start();
    }

    public Thread.State getWorkerThreadState() {
        return workerThread.getState();
    }

    public void resetWorker() {
        workerThread = null;
        workerThreadExceptionData = new ThreadExceptionData();
    }

}
