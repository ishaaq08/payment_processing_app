package com.ishaaq.consumer;

import com.ishaaq.app.BrokerConfig;

import javax.xml.crypto.Data;
import java.util.HashMap;
import java.util.Properties;
import java.sql.*;
import java.util.Random;

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

//        System.exit(0);
        // TESTING
        Random random = new Random();
        boolean partitionPaused = false;

        while (true) {
            // Check thread status
            if (myConsumer.workerThread == null) {
                System.out.println("MAIN: Worker thread has not yet been assigned a task.");
            } else if (myConsumer.workerThread.getState() == Thread.State.TERMINATED) {
                System.out.println("MAIN: Worker thread terminated. Checking worker thread status: " + myConsumer.workerThread.getState());
                System.out.println("MAIN: commiting offset");
                partitionPaused = false;
                System.out.println("MAIN: resuming partition --> partitionPaused = " + partitionPaused);
                System.out.println("MAIN: resetting thread");
                myConsumer.workerThread = null;
            } else {
                System.out.println("MAIN: Condition of workerThread = " + myConsumer.workerThread.getState());
            }

            // This mocks pause() if records have been returned
            if (partitionPaused) {
                System.out.println("MAIN: No records will be fetched --> partitionPaused = " + partitionPaused + "\n");
                Thread.sleep(5000);
                continue;
            }

            System.out.println("MAIN: Partition is not paused --> partitionPaused = " + partitionPaused);

            boolean success = random.nextBoolean();

            if (success) {
                System.out.println("MAIN: Records returned --> success == " + success);
                partitionPaused = true;
                System.out.println("MAIN: Partition paused --> partitionPaused = " + partitionPaused);

                Runnable task = () ->
                {
                    Thread.currentThread().setName("WORKER: ");
                    System.out.println(
                            Thread.currentThread().getName()
                                    + " running processing workload\n");
                    try {
                        Thread.sleep(10000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException("issue", e);
                    }
                };
                myConsumer.workerThread = new Thread(task);
                myConsumer.workerThread.start();
            } else {
                System.out.println("MAIN: no records returned. Waiting 5 seconds then continuing to next iteration.\n");
                Thread.sleep(5000);
            }
        }

    }
}
