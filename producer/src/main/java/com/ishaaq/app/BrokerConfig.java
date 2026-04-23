package com.ishaaq.app;

public class BrokerConfig {
    private String brokerAddress;

    public BrokerConfig() {
        String brokerAddressEnv = System.getenv("BROKER_ADDRESS");
        if (brokerAddressEnv != null) {
            // non-docker --> localhost:9092
            // docker --> broker:9093
            System.out.println("== Successfully creating BrokerConfig instance ==");
            System.out.println("--> updating brokerAddress with the value of the environment variable");
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
