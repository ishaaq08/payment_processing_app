package com.ishaaq.app;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.sound.midi.SysexMessage;
import java.rmi.server.UID;
import java.util.HashMap;
import java.util.Map;

public class Helpers {
    // Why static? I don't see the point of creating an object to do this
    public static String convertToJsonString(Object objectToConvert) {
        System.out.println("== Converting object to JSON string ==");
        System.out.println("--> NOTE: transaction_id will not be included in object as it is sent to the broker separately");

        ObjectMapper om = new ObjectMapper();

        try {
            // covert Java object to JSON strings
            String json = om.writeValueAsString(objectToConvert);

            System.out.println("--> object as string: " + json);

            return json;

        } catch (JsonProcessingException e) {
            System.out.println("--> failure converting object into a JSON string via the Jackson package!");
            throw new RuntimeException(e);
        }

    }

    // Returns hashmap where keys are strings and values are strings OR int
    public static Map<String, String> generateRandomTransaction() {
        System.out.println("== Generating random transaction ==");

        // Generate transaction_id
        UID transactionId = new UID();
        String transactionIdString = transactionId.toString();

//        System.out.println("UID: " + transactionIdString);

        // Some variables for generating random numbers
        int min = 1;
        int max = 10;
        int amount_min = 1;
        int amount_max = 100;

        // Initialise payor and payee before assigning value
        int payor = 0;
        int payee = 0;
        int iterNum = 0;

        while (iterNum == 0  || payor == payee) {
            payor = (int) ((Math.random() * (max - min)) + min);
            payee = (int) ((Math.random() * (max - min)) + min);

            iterNum++;
        }

        int amount = (int) ((Math.random() * (amount_max - amount_min)) + amount_min);

        Map <String, String> tranDetails = new HashMap<>();
        tranDetails.put("transaction_id", transactionIdString);
        tranDetails.put("payor", Integer.toString(payor));
        tranDetails.put("payee", Integer.toString(payee));
        tranDetails.put("amount", Integer.toString(amount));

        System.out.println("--> transaction details: " + tranDetails);


        return tranDetails;

    }
}
