package com.ishaaq.consumer;

import javax.xml.crypto.Data;
import java.util.HashMap;
import java.util.Properties;
import java.sql.*;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) {
//        BrokerConfig myBrokerConfig = new BrokerConfig();
//        String brokerAddress = myBrokerConfig.getBrokerAddress();
//
//        // Define configs
//        HashMap<String, Object> consumerConfigs = new HashMap<>();
//        consumerConfigs.put("bootstrap.servers", brokerAddress);
//        consumerConfigs.put("group.id", "test");
//        consumerConfigs.put("enable.auto.commit", "true");
//        consumerConfigs.put("auto.commit.interval.ms", "1000");
//        consumerConfigs.put("auto.offset.reset", "earliest"); // What to do when there is initial offset in Kafka e.g. when a consumer first subscribes
//        consumerConfigs.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        consumerConfigs.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//
//        // Create consumer class and pass in configs
//        AppConsumer myConsumer = new AppConsumer(consumerConfigs);
//
//        // Call the consumeMessage method
//        myConsumer.consumeMessages();

//        DatabaseOps myDB = new DatabaseOps("hello", "goodbye");
//        System.exit(0);

        String url = "jdbc:postgresql://localhost/payment_processing_db";
        Properties props = new Properties();
        props.setProperty("user", "postgres");
        props.setProperty("password", "superuser");

        DatabaseOps myDB = new DatabaseOps(props, url);

        // These will be accessed from the consumer record as we iterate through ConsumerRecords
        int payor_id = 8;
        int payee_id = 2;

        HashMap<Integer, Integer> balances = myDB.getPayorAndPayeeBalance(payor_id, payee_id);
        int payorCurrentBalance = balances.get(payor_id);
        int payeeCurrentBalance = balances.get(payee_id);
        System.out.println("The current balance of the payor is " + payorCurrentBalance);
        System.out.println("The current balance of the payee is " + payeeCurrentBalance);

        System.exit(0);

    }
}
