package com.ishaaq.app;

import org.apache.kafka.clients.producer.Producer; // Interface
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class AppProducer extends Builder<KafkaProducer<String,String>> {

    public AppProducer(Map<String, Object> configs) {
        super(configs);
    }

    // Abstract Method
    @Override
    public void createClient () {
        System.out.println("--> setting producer client to super.configs ");
        super.client = new KafkaProducer<>(super.configs);
    }

    public void sendMessage(String key, String payload, String topic) {
        System.out.println("== Sending message to topic partition ==");
        System.out.println("--> key: " + key);
        System.out.println("--> payload: " + payload);
        System.out.println("--> topic: " + topic);

        // Create ProducerRecord object from message details
        ProducerRecord<String, String> recordToSend = new ProducerRecord<>(topic, key, payload);

        // 4) Send record - SYNCHRONOUSLY
        RecordMetadata resultOfSend;
        try {
            resultOfSend = super.client.send(recordToSend).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.println("FAILED TO UPLOAD MESSAGE: transaction " + key);
            throw new RuntimeException("Kafka send interrupted", e);
        } catch (ExecutionException e) {
            System.out.println("FAILED TO UPLOAD MESSAGE: transaction " + key);
            throw new RuntimeException("Kafka send failed", e.getCause());
        }

        // Output message for successful uploads
        String ackdRecordTopic = resultOfSend.topic();
        long ackdRecordTimestamp = resultOfSend.timestamp();
        int ackdRecordPartition = resultOfSend.partition();
        int ackdRecordKeySize = resultOfSend.serializedKeySize();
        int ackdRecordValueSize = resultOfSend.serializedValueSize();

        System.out.println("--> UPLOAD SUCCESSFUL:\n*** Timestamp " + ackdRecordTimestamp + "\n*** Topic: " + ackdRecordTopic
                + "\n*** Partition: " + ackdRecordPartition + "\n*** Serialized key size: " + ackdRecordKeySize
                + "\n*** Serialised value size: " + ackdRecordValueSize
        );
    }
}
