package com.ishaaq.consumer;

import com.ishaaq.app.Builder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class AppConsumer extends Builder<KafkaConsumer<String, String>> {
    // Required to pass into process().
    // A single connection for a consumer, to re-use the same connections when processing records.
    private DatabaseOps dbConn;

    // Constructor
    public AppConsumer(Map<String, Object> consumerConfigs, Properties databaseConfigs, String databaseUrl) {
        super(consumerConfigs);
        dbConn = new DatabaseOps(databaseConfigs, databaseUrl);
    }

    // Abstract method override
    @Override
    public void createClient () {
        System.out.println("--> setting consumer client to super.configs ");
        super.client = new KafkaConsumer<>(super.configs);
    }

    public void consumeMessages() {
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
                // process() will be called here --> on each record in the batch
                System.out.printf("%noffset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            }
        }

    }

    public void processMessage(int payor_id, int payee_id) {
        // 1) Get payor and payee balance
        HashMap<Integer, Integer> balances = dbConn.getPayorAndPayeeBalance(payor_id, payee_id);
        int payorCurrentBalance = balances.get(payor_id);
        int payeeCurrentBalance = balances.get(payee_id);
        System.out.println("The current balance of the payor is " + payorCurrentBalance);
        System.out.println("The current balance of the payee is " + payeeCurrentBalance);
    }
}
