package com.ishaaq.app;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class PaymentEvent {
    // Instance variables != Class variables
    // Class variables are declared using static
    // This is the Kafka key
//    public String transaction_id;

    // --> Kafka record value
    public String amount;
    public String payee; // Unique 4 digit number
    public String payor; // Unique 4 digit number

    // Constructor - must be named the same as the name of the class
    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public PaymentEvent(
//            String transaction_id,
            @JsonProperty("amount") String amount,
            @JsonProperty("payee") String payee,
            @JsonProperty("payor") String payor
    )
    {
        // Use the 'this' keyword to reference the object
        // Since the params of the constructor have the same name as the instance variable the former will hide the latter due to scoping
        // 'this' allows differentiation as it specifically refers to the instance variables
//        this.transaction_id = transaction_id;
        this.amount = amount;
        this.payee = payee;
        this.payor = payor;
    }
}
