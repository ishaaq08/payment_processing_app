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
        consumerConfigs.put("auto.offset.reset", "earliest"); // What to do when there is initial offset in Kafka e.g. when a consumer first subscribes
        consumerConfigs.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerConfigs.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerConfigs.put("max.poll.records", "50");
        consumerConfigs.put("max.poll.interval.ms", "300");

        // Database configs
        String databaseUrl = "jdbc:postgresql://localhost/payment_processing_db";
        Properties databaseConfigs = new Properties();
        databaseConfigs.setProperty("user", "postgres");
        databaseConfigs.setProperty("password", "superuser");

        // Partitions


        // Create consumer class and pass in configs
        AppConsumer myConsumer = new AppConsumer(consumerConfigs, databaseConfigs, databaseUrl);

        // Call the consumeMessage method
        myConsumer.consumeMessages();

        // These will be accessed from the consumer record as we iterate through ConsumerRecords
//        int payor_id = 8;
//        int payee_id = 2;
//        int amount = 50;
//        String transactionId = "transaction-2";
//
//        myConsumer.processMessage(payor_id, payee_id, amount, transactionId);



    }
}
