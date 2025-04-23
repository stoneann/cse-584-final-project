package com.example;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.OutputStream;


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

        // Hadoop S3 config
        Configuration conf = new Configuration();
        conf.set("fs.s3a.endpoint", "s3.us-east-2.amazonaws.com");
        conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");

        FileSystem fs = FileSystem.get(new URI("s3a://584spark-east2/"), conf);
        int rowCount = 0;
        int fileIndex = 1;
        PrintWriter writer = null;
        OutputStream out = null;

        try {
            while (rs.next()) {
                // Start new file if needed
                if (rowCount % 100 == 0) {
                    // Close previous writer
                    if (writer != null) 
                    {
                        writer.close();
                    }
                    if (out != null) {
                        out.close();
                    }

                    // Create new file
                    String newFilePath = csvFilePath + fileIndex + ".csv";
                    Path outputPath = new Path(newFilePath);
                    out = fs.create(outputPath);
                    writer = new PrintWriter(out);

                    // Write header
                    for (int i = 1; i <= columnCount; i++) {
                        writer.print(meta.getColumnName(i));
                        if (i < columnCount) {
                            writer.print(",");
                        }
                    }
                    writer.println();

                    fileIndex++;
                }

                // Write row
                for (int i = 1; i <= columnCount; i++) {
                    String val = rs.getString(i);
                    if (val != null) {
                        val = val.replace("\"", "\"\"");
                        if (val.contains(",") || val.contains("\"")) {
                            val = "\"" + val + "\"";
                        }
                    }
                    writer.print(val != null ? val : "");
                    if (i < columnCount) writer.print(",");
                }
                writer.println();
                rowCount++;
            }
        } finally {
            // Final cleanup
            if (writer != null) {
                writer.close();
            }
            if (out != null) {
                out.close();
            }
        }
    }
}
