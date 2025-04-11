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

        Class.forName("io.prestosql.jdbc.PrestoDriver");

        String url = "jdbc:presto://" + prestoIp + ":8889/hive/default";
        Properties props = new Properties();
        props.setProperty("user", "emrtestuser");

        Connection conn = DriverManager.getConnection(url, props);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SET SESSION grouped_execution = true;\n" + 
                        "SET SESSION recoverable_grouped_execution = true;\n" + 
                        "SELECT * \n" + 
                        "FROM \"10_db\"." + tableOne + " \n" + 
                        "JOIN \"10_db\"." + tableTwo + " \n" +
                        "ON t1.join_id = t2.join_id\n");

        writeResultSetToCSV(rs, csvOutput);

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
