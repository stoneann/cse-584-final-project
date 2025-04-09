package com.example;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;


public class PrestoQuery {
    public static void main(String[] args) throws Exception {
        Class.forName("io.prestosql.jdbc.PrestoDriver");

        String url = "jdbc:presto://ip-172-31-36-243.us-east-2.compute.internal:8889/hive/default";
        Properties props = new Properties();
        props.setProperty("user", "emrtestuser");

        Connection conn = DriverManager.getConnection(url, props);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SET SESSION grouped_execution = true;\n" + //
                        "SET SESSION recoverable_grouped_execution = true;\n" + //
                        "\n" + //
                        "SELECT t1.join_id, t2.id, t1.id\n" + //
                        "FROM \"10_db\".l_table t1\n" + //
                        "JOIN \"10_db\".r_table t2\n" + //
                        "ON t1.join_id = t2.join_id\n");

        // ResultSet rs = stmt.executeQuery("SELECT * FROM \"10_db\".l_table");

        while (rs.next()) {
            System.out.println(rs.getString(1));
        }

        rs.close();
        stmt.close();
        conn.close();
    }
}
