package com.ishaaq.app;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.producer.Producer; // Interface


import java.util.HashMap;
import java.util.Map;

public abstract class Builder<T> {
    public Map<String, Object> configs;
    public T client;

    // Constructor
    public Builder(Map<String, Object> configs) {
        System.out.println("== Using Builder to create object ==");
        this.configs = configs;
        createClient();
    }

    // Getters
    public T getClient() {
        return client;
    }

    public Map<String, Object> getConfigs() {
        return new HashMap<>(configs); // defensive copy
    }

    // Methods
    public void close() {
        System.out.println("--> closing client");
    }

    // Private/helper methods
    public abstract void createClient();
}
