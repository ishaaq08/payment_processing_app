package com.ishaaq.consumer;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;
import org.javatuples.Pair;

public class DatabaseOps {
    // Instance variables: all the below are private
    private Properties info; // this should at least contain user & password
    private String url;
    private Connection conn;

    public DatabaseOps (Properties info, String url) {
        System.out.println("=== Creating DatabaseOps Object ===");
        this.info = info;
        this.url = url;
        this.conn = getConn();
    }

    /*
    public METHOD: getPayorAndPayeeCurrentBalanceSingleRead

        params:
            int payor
            int payee

        returns:
            tuple(payee, payor)
            OR
            HashMap<1: 200, 2:300)

        process:
            - define query
            - pass the query into getPreparedStatement
            - invoke the executeQuery() method on the prepared statement
            - return the balance of the payee and payor

            The key needs to be the userID

            payorCurrentBalance = HashMap.get(payorUserId)
            payeeCurrentBalance = HashMap.get(payeeUserId)

     */

    public void getBalance(int payor_id) {
        String sqlQuery = "SELECT balance FROM tbl_balance WHERE user_id = ?;";
        ArrayList<Object> arguments = new ArrayList<>(Arrays.asList(5));
        PreparedStatement pstmtGet = getPreparedStatement(sqlQuery, arguments);

        try {
            ResultSet pstmtResults = pstmtGet.executeQuery();

            int payorBalance;

            while (pstmtResults.next()) {
                payorBalance = pstmtResults.getInt(1);
                System.out.println(payor_id + " has a balance of " + payorBalance);}

            pstmtGet.close();
        } catch (SQLException e) {
            System.out.println("This error originates from the executeQuery, next or getInt methods.");
            e.printStackTrace();
            throw new RuntimeException("Error when performing database operations: " + e);
        }

    }

    private PreparedStatement getPreparedStatement(String sql, ArrayList<Object> sqlArgs ) {
        PreparedStatement pstmtGet;

        try{
            pstmtGet = conn.prepareStatement(sql);

            // Insert arguments into SQL query
            for (int index = 0; index < sqlArgs.size(); index++) {
                if (sqlArgs.get(index) instanceof Integer) {
                    pstmtGet.setInt(index+1, (Integer) sqlArgs.get(index));
                // Exclusively for the update transaction method
                } else if (sqlArgs.get(index) instanceof String) {
                    pstmtGet.setString(index+1, (String) sqlArgs.get(index));
                }
            }

        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("Error performing database operation", e);
        } catch (Exception e) {
            System.out.println("Some other error occurred");
            throw new RuntimeException(e);
        }
        return pstmtGet;
    }

    // private method: get connection
    private Connection getConn() {
        System.out.println("--> Generating connection to DB");
        try{
            conn = DriverManager.getConnection(url, info);
        } catch (SQLException e){
            e.printStackTrace();
            throw new RuntimeException("Database access error. Not due to null url as this is checked via constructor", e);
        }
        return conn;

    }
}
