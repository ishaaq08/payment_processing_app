package com.ishaaq.consumer;

import java.util.HashMap;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) {
        // Define configs
        HashMap<String, String> consumerConfigs = new HashMap<>();
        consumerConfigs.put("bootstrap.servers", "localhost:9092");
        consumerConfigs.put("group.id", "test");
        consumerConfigs.put("enable.auto.commit", "true");
        consumerConfigs.put("auto.commit.interval.ms", "1000");
        consumerConfigs.put("auto.offset.reset", "earliest"); // What to do when there is initial offset in Kafka e.g. when a consumer first subscribes
        consumerConfigs.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerConfigs.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        
        // Create consumer class and pass in configs
        
        // Call the consumeMessage method
    }
}
