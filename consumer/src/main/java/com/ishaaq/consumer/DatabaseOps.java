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
            HashMap<payor_id: 200, payee_id:300)

        how the variable that calls this method will store the return value:
            payorCurrentBalance = HashMap.get(payorUserId)
            payeeCurrentBalance = HashMap.get(payeeUserId)

     */

    public HashMap<Integer, Integer> getPayorAndPayeeBalance(int payor_id, int payee_id) {
        String sqlQuery = "SELECT user_id, balance FROM tbl_balance WHERE user_id in (?, ?);";
        ArrayList<Object> arguments = new ArrayList<>(Arrays.asList(payor_id, payee_id));
        int payorCurrentBalance;
        int payeeCurrentBalance;
        HashMap<Integer, Integer> balances = new HashMap<>();

        try (
            PreparedStatement pstmt = getPreparedStatement(sqlQuery, arguments);
             ResultSet rs = pstmt.executeQuery()
        ) {

            while (rs.next()) {
                int userId = rs.getInt("user_id");
                int balance = rs.getInt("balance");
                balances.put(userId, balance);
            }

        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("Error when performing database operations: " + e);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("An non-database error has occured: " + e);
        }

        return balances;

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
