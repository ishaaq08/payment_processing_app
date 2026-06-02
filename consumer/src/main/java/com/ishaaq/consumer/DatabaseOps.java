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

        // VALIDATION: Not a fan of the manual definition of tables in sqlQuery. Not scalable
        if (Objects.equals(table, "tbl_balance")) {
            sqlQuery = "UPDATE tbl_balance SET balance = ? WHERE user_id = ?;";
        } else if (Objects.equals(table, "tbl_transactions")) {
            sqlQuery = "UPDATE tbl_transactions SET status = ? WHERE transaction_id = ?;";
        }

        ArrayList<Object> arguments = new ArrayList<>(Arrays.asList(newValue, rowId));

        try (PreparedStatement pstmt = getPreparedStatement(sqlQuery, arguments);){
            int updateResult;
            updateResult = pstmt.executeUpdate();

        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("Error performing database operation executeUpdate()", e);
        }
        System.out.println("--> Successfully updated table " + table + " where rowId equals " + rowId);

    }

    public void insertTransaction(int payor, int payee, int amount, String transactionId, String status) {
        System.out.println("== inserting transaction into db ==");

        // Define SQL query
        String sqlQuery = "INSERT INTO tbl_transactions(transaction_id, payor, payee, amount, status) VALUES (?,?,?,?,?);";

        // SQL query arguments
        ArrayList<Object> arguments = new ArrayList<>(Arrays.asList(transactionId, payor, payee, amount, status));

        // try-with-resources
        try (PreparedStatement pstmt = getPreparedStatement(sqlQuery, arguments);) {
            int updateResult = pstmt.executeUpdate();

            // Validate the update
            if (updateResult ==1) {
                System.out.printf("--> successfully inserted transaction %s%n", transactionId);
            } else {
                throw new RuntimeException("Encountered issue during executeUpdate(). Expected 1 to be returned.");
            }

        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("Error performing database operation executeUpdate()", e);
        }
    }

    public void commitTransaction() {
        System.out.println("--> ℹ️ comitting transaction to database");
        
        try {
            conn.commit();
        } catch (SQLException e) {
            throw new RuntimeException("Error when committing transaction. This can occur due to a variety of reasons", e);
        }
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

            // debugging
            System.out.println(pstmtGet);


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
        System.out.println("--> ℹ️ Generating connection to DB");
        try{
            conn = DriverManager.getConnection(url, info);
            System.out.println("--> setting auto commit to false");
            conn.setAutoCommit(false);
        } catch (SQLException e){
            e.printStackTrace();
            throw new RuntimeException("Database access error. Not due to null url as this is checked via constructor", e);
        }
        return conn;

    }
}
