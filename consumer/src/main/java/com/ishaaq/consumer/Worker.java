package com.ishaaq.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecords;

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
        System.out.println("--> setting up worker thread");
        Task processingTask = new Task(dbConn, records, nextBatchSizeToInsert);
        workerThread = new Thread(processingTask);
        workerThread.setUncaughtExceptionHandler(new WorkerExceptionHandler(workerThreadExceptionData));
    }

    public void runWorkerThread() {
        System.out.println("--> running worker thread");
        workerThread.start();
    }

    public Thread.State getWorkerThreadState() {
        if (workerThread == null) {
            return null;
        } else {
            return workerThread.getState();
        }
    }

    public void resetWorker() {
        workerThread = null;
        workerThreadExceptionData = new ThreadExceptionData();
    }

}
