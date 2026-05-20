package com.ishaaq.consumer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Properties;

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

    /*
    private METHOD: getPreparedStatement

        params:
            String sqlString
            Hashmap<Integer, Object> queryArgs: keys will be increasing integers

         return:
            PreparedStatement pstmt: the public methods (getPayorAndPayeeCurrentBalance
    */
//    private ResultSet getPreparedStatement(String sql, HashMap<Integer, Object> ) {
//
//    }

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
