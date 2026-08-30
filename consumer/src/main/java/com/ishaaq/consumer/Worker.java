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
    /**
     * After a batch of records has been received by the consumer this method will create the worker thread. An instance
     * of the Task class will be created, which implements the Runnable interface, which will be used to create a Thread
     * object. The uncaughtExceptionHandler of the Thread object is also set so that if an exception is thrown by the
     * worker thread, the field workerThreadExceptionData will be updated.
     *
     * @param dbConn Connection to the postgres database. Part of processing the records includes inserting them into
     *              the database, thus the database connection is crucial.
     * @param records The records retrieved from the last poll call which will be processed by the worker thread.
     * @param nextBatchSizeToInsert The number of records retrieved in the last poll call. Processing also includes updating
     *                              the batch metrics table with data about how long a batch of x records takes to process.
     *
     */
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
