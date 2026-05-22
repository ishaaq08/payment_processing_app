package com.ishaaq.consumer;
import java.sql.*;
import java.util.*;

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

    // ====== PUBLIC METHODS ======

    public HashMap<Integer, Integer> getPayorAndPayeeBalance(int payorId, int payeeId) {
        String sqlQuery = "SELECT user_id, balance FROM tbl_balance WHERE user_id in (?, ?);";
        ArrayList<Object> arguments = new ArrayList<>(Arrays.asList(payorId, payeeId));
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
            throw new RuntimeException("An non-database error has occurred: " + e);
        }

        return balances;

    }

    public void performUpdate(String table, int newValue, int rowId) {
        System.out.println("== Performing update on table " + table + " ==");
        String sqlQuery = "";

        // VALIDATION: check if table is equal to tbl_balance OR tbl_transactions

        if (Objects.equals(table, "tbl_balance")) {
            sqlQuery = "UPDATE ? SET balance = ? WHERE user_id = ?;";
        } else if (Objects.equals(table, "tbl_transactions")) {
            sqlQuery = "UPDATE ? SET status = ? WHERE transaction_id = ?;";
        }

        // String sqlQuery2 = "UPDATE tbl_transactions SET status = ? WHERE transaction_id = ?;";
        ArrayList<Object> arguments = new ArrayList<>(Arrays.asList(table, newValue, rowId));

        PreparedStatement pstmt = getPreparedStatement(sqlQuery, arguments);
        // Use this result to check if the update has been performed i.e. if the result is equals to 0
        int updateResult;

        try{
            updateResult = pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("Error performing database operation", e);
        }

        System.out.println("--> Successfully updated table " + table + " where rowId equals " + rowId);

    }

    // ====== PRIVATE METHODS ======

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
