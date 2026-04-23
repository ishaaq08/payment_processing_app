package com.ishaaq.app;

public class BrokerConfig {
    private String brokerAddress;

    public BrokerConfig() {
        String brokerAddressEnv = System.getenv("BROKER_ADDRESS");
        if (brokerAddressEnv != null) {
            System.out.println("--> Successfully retrieved environment variable BROKER_ADDRESS");
            System.out.println("--> access via the instance variable brokerAddress");
            this.brokerAddress = brokerAddressEnv;
        } else {
            throw new RuntimeException("The environment variable BROKER_ADDRESS has not been set. This is required when " +
                    "connecting to the Kafka cluster. Failed to creater instance of BrokerConfig. Please set the environment" +
                    " variable then try again.");
        }
    }

    public String getBrokerAddress() {
        System.out.println("--> retrieving broker address: " + brokerAddress);
        return brokerAddress;
    };
}
