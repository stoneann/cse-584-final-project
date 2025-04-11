package com.example;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Properties;


public class PrestoQuery {
    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: PrestoQuery <table_one_name> <table_two_name> <csv_output_path> <ip-XXXX.us-east-2.compute.internal>");
            System.exit(1);
        }
        String tableOne = args[0];
        String tableTwo = args[1];
        String csvOutput = args[2];
        String prestoIp = args[3];
        System.out.println("Table 1: " + tableOne);
        System.out.println("Table 2: " + tableTwo);
        System.out.println("Output: " + csvOutput);
        System.out.println("Host: " + prestoIp);

        Class.forName("io.prestosql.jdbc.PrestoDriver");

        String url = "jdbc:presto://" + prestoIp + ":8889/hive/default";
        Properties props = new Properties();
        props.setProperty("user", "emrtestuser");

        Connection conn = DriverManager.getConnection(url, props);
        Statement geStmt = conn.createStatement();
        geStmt.execute("SET SESSION grouped_execution = true");
    

        System.out.println("Executed Grouped Execution");

        Statement rgeStmt = conn.createStatement();
        rgeStmt.execute("SET SESSION recoverable_grouped_execution = true");
        System.out.println("Executed Recovery Grouped Execution");


        String sqlQuery = "SELECT * FROM \"10_db\"." + tableOne + " t1 JOIN \"10_db\"." + tableTwo + " t2 ON t1.join_id = t2.join_id";
        System.out.println(sqlQuery);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sqlQuery);

        System.out.println("Executed Join");

        writeResultSetToCSV(rs, csvOutput);

        System.out.println("Wrote to CSV");

        rs.close();
        stmt.close();
        conn.close();
    }

    public static void writeResultSetToCSV(ResultSet rs, String csvFilePath) throws Exception {
        ResultSetMetaData meta = rs.getMetaData();
        int columnCount = meta.getColumnCount();

        try (PrintWriter writer = new PrintWriter(new FileWriter(csvFilePath))) {
            // Write header
            for (int i = 1; i <= columnCount; i++) {
                writer.print(meta.getColumnName(i));
                if (i < columnCount) writer.print(",");
            }
            writer.println();

            // Write rows
            while (rs.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    String value = rs.getString(i);
                    if (value != null) {
                        // Escape quotes
                        value = value.replace("\"", "\"\"");
                        // Wrap in quotes if contains comma or quote
                        if (value.contains(",") || value.contains("\"")) {
                            value = "\"" + value + "\"";
                        }
                    }
                    writer.print(value != null ? value : "");
                    if (i < columnCount) writer.print(",");
                }
                writer.println();
            }
        }
    }
}
