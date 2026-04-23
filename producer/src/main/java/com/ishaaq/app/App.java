package com.ishaaq.app;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.producer.KafkaProducer;

public class App {
    public static void main(String[] args) throws InterruptedException {
        // Access environment variable for broker_address
        final String brokerAddress = System.getenv("BROKER_ADDRESS");
        if (brokerAddress != null) {
            // Script ran directly = localhost:9092
            // Script ran via docker = broker:9093
            System.out.println("--> Successfully retrieved environment variable BROKER_ADDRESS: " + brokerAddress);
        } else {
            throw new RuntimeException("The environment variable BROKER_ADDRESS has not been set. This is required when " +
                    "connecting to the Kafka cluster. Please set the environment variable then try again.");
        }

        // Admin Configs
        Map<String, Object> configs = new HashMap<>();
        configs.put("bootstrap.servers", brokerAddress);
        configs.put("metadata.recovery.strategy", "NONE");


        // Admin setup
        AppAdmin myAdmin = new AppAdmin(configs);
        Admin myAdminClient = myAdmin.getClient();

        // Create Topic object and make topics
        String topicName = "payments";
        Topic myTopic = new Topic(myAdminClient, topicName, 1, (short) 1); // It would be cool if this took in a HashMap{topicName, numOfPartitions, replicationFactor}
        myTopic.createTopic();
        myTopic.viewAllTopics();

        // Producer configurations
        Map<String, Object> producerConfigs = new HashMap<>();
        producerConfigs.put("bootstrap.servers", brokerAddress);
        producerConfigs.put("linger.ms", 1);
        producerConfigs.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerConfigs.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerConfigs.put("metadata.recovery.strategy", "NONE");

        // Create producer object
        AppProducer myProducer = new AppProducer(producerConfigs);

        for (int i=1; i < 30; i++) {
            System.out.println("=========================================");

            // Generate one random transaction
            Map<String, String> transactionDetails = Helpers.generateRandomTransaction();

            // Store key
            String key = transactionDetails.get("transaction_id");

            // Create PaymentEvent from the transaction
            PaymentEvent transactionEvent = new PaymentEvent(
                    transactionDetails.get("amount"),
                    transactionDetails.get("payee"),
                    transactionDetails.get("payor")
            );

            // Convert the PaymentEvent to a JSON string
            String payload = Helpers.convertToJsonString(transactionEvent);

            // Upload transaction to partition
            myProducer.sendMessage(key, payload, topicName);

            Thread.sleep(5000);
        }

    }
}