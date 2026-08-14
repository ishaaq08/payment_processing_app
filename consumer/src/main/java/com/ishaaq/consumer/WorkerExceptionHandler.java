package com.ishaaq.consumer;

public class WorkerExceptionHandler implements Thread.UncaughtExceptionHandler {
    private ThreadExceptionData workerThreadExceptionData;

    WorkerExceptionHandler(ThreadExceptionData workerThreadExceptionData) {
        this.workerThreadExceptionData = workerThreadExceptionData;
    }

    @Override
    public void uncaughtException(Thread t, Throwable e) {
         workerThreadExceptionData.setThreadException(new RuntimeException("Worker thread has failed!", e));
    }
}
