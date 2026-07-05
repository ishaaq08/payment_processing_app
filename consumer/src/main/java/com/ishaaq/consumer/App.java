package com.ishaaq.consumer;

import com.ishaaq.app.BrokerConfig;

import javax.xml.crypto.Data;
import java.util.HashMap;
import java.util.Properties;
import java.sql.*;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) throws InterruptedException {
        BrokerConfig myBrokerConfig = new BrokerConfig();
        String brokerAddress = myBrokerConfig.getBrokerAddress();

        // Define configs
        HashMap<String, Object> consumerConfigs = new HashMap<>();
        consumerConfigs.put("bootstrap.servers", brokerAddress);
        consumerConfigs.put("group.id", "test");
        consumerConfigs.put("enable.auto.commit", "true");
        consumerConfigs.put("auto.commit.interval.ms", "1000");
        consumerConfigs.put("auto.offset.reset", "earliest");
        consumerConfigs.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerConfigs.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerConfigs.put("max.poll.records", "50");
        consumerConfigs.put("max.poll.interval.ms", "300");

        // Database configs
        String databaseUrl = "jdbc:postgresql://localhost/payment_processing_db";
        Properties databaseConfigs = new Properties();
        databaseConfigs.setProperty("user", "postgres");
        databaseConfigs.setProperty("password", "superuser");

        // Create consumer class and pass in configs
        AppConsumer myConsumer = new AppConsumer(consumerConfigs, databaseConfigs, databaseUrl);

        // Call the consumeMessage method
//        myConsumer.consumeMessages();

        while (true) {
            // Check thread status - create instance field first of the type Thread

            // Create randomised condition to check if records have been returned

            // define statement, if true then spin up the worker thread, sleep statement

            //
        }

    }
}
