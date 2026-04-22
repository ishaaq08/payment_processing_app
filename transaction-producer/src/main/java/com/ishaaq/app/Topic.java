package com.ishaaq.app;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ExecutionException;


public class Topic {
    // Instance variable(s)
    private Admin admin;
    private String topicName;
    private int numOfPartitions;
    private short replicationFactor;

    // Constructor
    Topic (Admin admin, String topicName, int numOfPartitions, short replicationFactor) {
        this.admin = admin;
        this.topicName = topicName;
        this.numOfPartitions = numOfPartitions;
        this.replicationFactor = replicationFactor;
    }

    // createTopic
    public void createTopic() {
        System.out.println("== Creating topic ==");
        try {
            // Creating NewTopic object
            System.out.println("--> Topic to be created: " + topicName);
            NewTopic topicToCreate = new NewTopic(this.topicName, this.numOfPartitions, this.replicationFactor);
            CreateTopicsResult createTopicsResult = this.admin.createTopics(Collections.singleton(topicToCreate));

            // Retrieve the result
            KafkaFuture<Void> future = createTopicsResult.values().get(this.topicName); // This returns the value for the key in the map
            future.get(); // Waits for the future to complete and then gets the result --> void in this case

            System.out.println("--> Successfully created topic: " + this.topicName + "\n");


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // viewAllTopics
    public void viewAllTopics() {
        System.out.println("== Viewing all topics ==");
        // 1) Call the listTopics method
        ListTopicsResult allTopics = this.admin.listTopics();

        // 2) FUTURE: Access allTopics
        KafkaFuture<Set<String>> allTopicsFuture = allTopics.names();

        // 3) Get the names of the topics
        // --> The get() method can throw the exceptions 1) InterruptedException 2) ExecutedException
        try {
            Set<String> allTopicsNames = allTopicsFuture.get();

            // 4) Print the names of the topics
            int counter = 1;
            for (String key : allTopicsNames) {
                System.out.format("--> Topic %s: %s\n", counter, key);
                ++counter;
            }

        } catch (InterruptedException e) {
            System.out.println("Encountered InterruptedException. Whilst the thread was waiting, sleeping or otherwise occupied" +
                    " it was interrupted.");
            e.printStackTrace();
        } catch (ExecutionException e) {
            System.out.println("Exception thrown when attempting to retrieve the result of a task that aborted by throwing an exception");
            e.printStackTrace();
        }


    }
}
