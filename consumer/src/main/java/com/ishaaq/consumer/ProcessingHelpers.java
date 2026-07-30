package com.ishaaq.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ishaaq.app.PaymentEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.HashMap;

public class ProcessingHelpers {
    private static ObjectMapper objectMapper = new ObjectMapper();
    public static record RecordDetails(int payorId, int payeeId, int amount, String transactionId) {};

    /**
     * Parse a ConsumerRecord and return its key and payload as a RecordDetails record. Jackson converts
     * the JSON string from the payload into a PaymentEvent object, and its fields populate the returned
     * RecordDetails.
     *
     * @param record: The ConsumerRecord to be parsed
     * @return recordDetails: a RecordDetails record containing payorId, payeeId, amount (all int), and transactionId (String)
     */
    static public RecordDetails parseConsumerRecord(ConsumerRecord<String, String> record) {
        System.out.println("--> entering method: parsing record details");

        String transactionId = record.key();
        String payloadJson = record.value();
        PaymentEvent payloadAsPaymentEvent;

        try{
            payloadAsPaymentEvent = objectMapper.readValue(payloadJson, PaymentEvent.class);
        } catch (JsonProcessingException e) {
            System.out.println("--> failure converting JSON string into an object!");
            throw new RuntimeException(e);
        }

        RecordDetails recordDetails = new RecordDetails(
                Integer.parseInt(payloadAsPaymentEvent.payor),
                Integer.parseInt(payloadAsPaymentEvent.payee),
                Integer.parseInt(payloadAsPaymentEvent.amount),
                transactionId
        );

        System.out.printf("--> offset %s: transaction %s : user %s transfers user %s an amount of £%s%n", record.offset(),
                transactionId, recordDetails.payorId(), recordDetails.payeeId(), recordDetails.amount());

        return recordDetails;
    }

    /**
     * The method responsible for processing a single ConsumerRecord. It is called within the for each loop in
     * consumeMessages to process a single record.

     * @param payorId: The ID of the payor
     * @param payeeId: The ID of the payee
     * @param amount: The potential amount to be transferred from the payor to the payee
     * @param transactionId: The unique ID of the transaction
     *
     * @throws RuntimeException if there is an error when processing the message. The error likely comes from any of the
     * methods invoked by dbConn. These methods throw a SQLException which are propagated up the stack as a RuntimeException.
     * Those RuntimeExceptions are caught here.
     */
    static public void processMessage(DatabaseOps dbConn, int payorId, int payeeId, int amount, String transactionId) {
        System.out.println("--> entering method: processing message");

        boolean doesTransactionExist = dbConn.transactionExists(transactionId);

        if (doesTransactionExist) {
            System.out.println("--> transaction exists");
        } else{
            HashMap<Integer, Integer> balances = dbConn.getPayorAndPayeeBalance(payorId, payeeId);
            int payorCurrentBalance = balances.get(payorId);
            int payeeCurrentBalance = balances.get(payeeId);
            boolean sufficient = payorCurrentBalance >= amount;

            try {
                transferFunds(dbConn, sufficient, payorId, payorCurrentBalance, payeeId, payeeCurrentBalance, amount, transactionId);
                dbConn.commitTransaction();

            } catch (Exception e) {
                dbConn.rollbackTransaction();
                throw new RuntimeException("Encountered exception when processing message", e);
            }
        }

    }

    /**
     * Method takes 1 of 2 flows depending on the argument passed to the 'sufficient' parameter
     *<p><ol>
     *     <li>'sufficient' = true: Payor has sufficient funds. Update balances of payor and payee in tbl_balance. Insert
     *      transaction into tbl_transactions with an "ACCEPTED" status.</li>
     *      <li> 'sufficient' = false: Payee has insufficient funds. Insert transaction into tbl_transactions with a "DENIED"
     *      * status. No update of balances.</li>
     *</ol></p>

     * @param sufficient: Whether the payor has sufficient funds to complete the transaction
     * @param payorId: The ID of the payor
     * @param payorCurrentBalance: The current balance of the payor
     * @param payeeId: The ID of the payee
     * @param payeeCurrentBalance: The current balance of the payee
     * @param amount: The potential amount to be transferred from the payor to the payee
     * @param transactionId: The unique ID of the transaction
     */
    static private void transferFunds(DatabaseOps dbConn, boolean sufficient, int payorId, int payorCurrentBalance, int payeeId,
                               int payeeCurrentBalance, int amount, String transactionId) {
        if (sufficient) {
            System.out.printf("--> payor %s has sufficient funds, updating balances and inserting transaction%n", payorId);
            int payorNewBalance = payorCurrentBalance - amount;
            int payeeNewBalance = payeeCurrentBalance + amount;
            dbConn.performUpdate("tbl_balance", payorNewBalance, payorId);
            dbConn.performUpdate("tbl_balance", payeeNewBalance, payeeId);
            dbConn.insertTransaction(payorId, payeeId, amount, transactionId, "ACCEPTED");
        } else {
            System.out.printf("--> payor %s has insufficient funds, no balance update needed and inserting transaction%n", payorId);
            dbConn.insertTransaction(payorId, payeeId, amount, transactionId, "DENIED");
        }

    }

}
