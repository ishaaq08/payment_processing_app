package com.ishaaq.consumer;

import com.ishaaq.app.Builder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Map;

public class AppConsumer extends Builder<KafkaConsumer<String, String>> {

    // Constructor
    public AppConsumer(Map<String, Object> configs) {
        super(configs);
    }

    // Abstract method override
    @Override
    public void createClient () {
        System.out.println("--> setting consumer client to super.configs ");
        super.client = new KafkaConsumer<>(super.configs);
    }

    public void consumeMessage() {
        // How are partitions assigned in production?
        // For testing, we will just do manual assignment of partitions
        System.out.println("== Consuming message from partition ==");
        System.out.println("--> consuming message from payments-0");
        TopicPartition partition0 = new TopicPartition("payments", 0);
        ArrayList<TopicPartition> allPartitions = new ArrayList<>();
        allPartitions.add(partition0);
        super.client.assign(allPartitions);

        while (true) {
            ConsumerRecords<String, String> records = super.client.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("%noffset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            }
        }

    }
}
