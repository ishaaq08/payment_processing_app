package com.ishaaq.consumer;

import com.ishaaq.app.Builder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.sql.SQLException;
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

    // Eventually this class will receive the entire payload of the event
    public void processMessage(int payorId, int payeeId, int amount, String transactionId) {
        System.out.println("== Processing partition message ==");
        // Update DatabaseOps so that by defualt auto commit is set to false

        // Check if the transaction already exists in the database

        // TRUE: Transaction exists
        // 1) commit offset by 1
        // 2) return from method
        // NOTE: The transaction already exists but the offset hasn't been committed because the system must have crashed
        // ...after the db transaction was committed but before the partition offset was committed.

        // FALSE --> Transaction doesn't exist
        HashMap<Integer, Integer> balances = dbConn.getPayorAndPayeeBalance(payorId, payeeId);
        int payorCurrentBalance = balances.get(payorId);
        int payeeCurrentBalance = balances.get(payeeId);
        System.out.printf("payor balance: %s, payee balance: %s%n", payorCurrentBalance, payeeCurrentBalance);

        // Check if the payor has sufficient balance i.e. compare to transaction
        boolean sufficient = payorCurrentBalance > amount;
        System.out.println("--> does the payor have sufficient funds: " + sufficient);

        // == BEGINNING OF DATABASE TRANSACTION ==
        try {
            if (sufficient) {
                // User has sufficient funds
                // Update balances
                System.out.println("--> performing balance transfers");
                int payorNewBalance = payorCurrentBalance - amount;
                int payeeNewBalance = payeeCurrentBalance + amount;
                dbConn.performUpdate("tbl_balance", payorNewBalance, payorId);
                dbConn.performUpdate("tbl_balance", payeeNewBalance, payeeId);

                // Insert transaction with "ACCEPTED" status
                dbConn.insertTransaction(payorId, payeeId, amount, transactionId, "ACCEPTED");

            } else {
                // User has insufficient funds and no balance updates required
                // Insert transaction with "DENIED" status
                dbConn.insertTransaction(payorId, payeeId, amount, transactionId, "DENIED");
            }

            // == END OF DB TRANSACTION ==
            dbConn.commitTransaction();
            // to-add: commit partition offset

            System.out.println("✅ Successfully processed messaged: db transaction and partition offset comnitted.");

        } catch (Exception e) {
            // The method performUpdate() and insertTransaction() will catch a SQLException. They will throw a RuntimeException
            // ... which will be caught here. We can't explicitly specify a SQLException in this catch block because none of
            // ... the above logic throws it
            dbConn.rollbackTransaction();
            throw new RuntimeException("Encountered exception when processing message", e);
        }
        // Later we can close the connection - no need to add right now

    }
}
