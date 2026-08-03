package com.ishaaq.consumer;

public class WorkerExceptionHandler implements Thread.UncaughtExceptionHandler {


    @Override
    public void uncaughtException(Thread t, Throwable e) {
        // consumerWorker.workerThreadExceptionData.setThreadException(e);
    }
}
