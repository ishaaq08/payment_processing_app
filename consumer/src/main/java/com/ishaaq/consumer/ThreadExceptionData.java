package com.ishaaq.consumer;

public class ThreadExceptionData {
    private boolean threadWithoutException = true;
    private Exception threadException = null;

    // default constructor implicitly applied

    // Getters
    // -- For boolean getters, best practice is to prefix the field with is in the method name
    public boolean isThreadWithoutException() {
        return threadWithoutException;
    }

    public Exception getThreadException() {
        return threadException;
    }

    // Setter
    public void setThreadException(Exception e) {
        threadWithoutException = true;
        threadException = e;
    }
}
