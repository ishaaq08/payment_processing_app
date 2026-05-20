package com.ishaaq.consumer;

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

        // All of these will be stored in the ConsumerRecord
        int amount = 400;
        int payor = 2; // 500
        int payee = 8;  // 700
        try{
            Connection conn = DriverManager.getConnection(url, props);

            // GET balance
            // Method getPayorBalance
            // maybe a single query returning multiple results???
            // params: payor OR payee
            // returns: int balance
            String getPayorBalanceQuery = "SELECT user_id, balance FROM tbl_balance WHERE user_id in (?, ?);";
            PreparedStatement pstmtGet = conn.prepareStatement(getPayorBalanceQuery);
            pstmtGet.setInt(1, payor); // this will be the payor
            pstmtGet.setInt(2, payee); // this will be the payee
            ResultSet rsGetPayorBalanceQuery = pstmtGet.executeQuery();

            int payorCurrentBalance;
            int payeeCurrentBalance;

            int currentBalance = 0;
            while (rsGetPayorBalanceQuery.next()) {
                int userId = rsGetPayorBalanceQuery.getInt(1);
                if (userId == payor) {
                    payorCurrentBalance = rsGetPayorBalanceQuery.getInt(2);
                    System.out.println("The current balance for the payor " + payor + " is £" + payorCurrentBalance); // expect 500
                } else {
                    payeeCurrentBalance = rsGetPayorBalanceQuery.getInt(2);
                    System.out.println("The current balance for the payee " + payee + " is £" + payeeCurrentBalance); // expect 700
                }
            }
            rsGetPayorBalanceQuery.close();
            pstmtGet.close();

            System.exit(0);

            // COMPARE payorCurrentBalance to amount


            // --> payorCurrentBalance > amount
                // 1) deduct amount from payorCurrentBalance in db
                // 2) add amount to payeeCurrentBalance in db
                // 3) update status of transction in db
            // store in a method: approvedTransaction
            // for steps 1 and 2, since this will be repeated code this should be in a method updateBalance


            // --> payorCurrentBalance < amount
                // 1) update status of transaction
                // 2) send an alert (TBC)
            // store in a method: deniedTransaction


            // COMPARE the payor's balance to the payment amount
            if (currentBalance > amount) {
                // Sufficient balance
                System.out.println("ACCEPTED --> Sufficient funds");

                // UPDATE the payors balance
                // method updateBalance
                // params: payor, payee
                int updatedBalance = currentBalance - amount;
                String updatePayorBalanceQuery = "UPDATE tbl_balance SET balance=? WHERE user_id=?;";
                PreparedStatement pstmtUpdate = conn.prepareStatement(updatePayorBalanceQuery);
                pstmtUpdate.setInt(1, updatedBalance);
                pstmtUpdate.setInt(2, payor);

                System.out.println("Updating balance to: " + updatedBalance);
                pstmtUpdate.executeUpdate();

                // UPDATE the payee's balance

            } else {
                // Insufficient balance
                System.out.println("DECLINED --> Insufficient funds");

            }

//                int userBalance = rs.getInt(1);
//                if (amount < userBalance) {
//                    System.out.println("STATUS: ACCEPTED");
//                    int newBalance = userBalance - amount;
//                    System.out.println("New balance: " + newBalance);
//                    ResultSet rsUpdate = st.executeUpdate("UPDATE tbl_balance SET balance=x WHERE user_id=9;");
//                } else {
//                    System.out.println("STATUS: REJECTED --> user has insufficient funds: " + userBalance);
//                }

        } catch (SQLException e) {
            e.printStackTrace();
        }


    }
}
