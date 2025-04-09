package com.example;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;


public class PrestoQuery {
    public static void main(String[] args) throws Exception {
        // final String JDBC_DRIVER = "com.facebook.presto.jdbc.PrestoDriver";
        
        // Class.forName(JDBC_DRIVER); 
        
        String url = "jdbc:presto://ec2-13-58-144-197.us-east-2.compute.amazonaws.com:8889/hive/default";
        // String sql = new String(java.nio.file.Files.readAllBytes(java.nio.file.Paths.get("grouped_join.sql")));
        String sql_query = "SET SESSION grouped_execution = true; SET SESSION recoverable_grouped_execution = true; SELECT t1.join_id, t2.id, t1.id FROM \"10_db\".l_table t1 JOIN \"10_db\".r_table t2 ON t1.join_id = t2.join_id;";
        try (Connection conn = DriverManager.getConnection(url, "aryanj", null);
             Statement stmt = conn.createStatement()) {
            stmt.execute(sql_query);
            System.out.println("Query executed successfully.");
        }
    }
}
