package org.spark.test;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class SimpleJoinTest {
    private static SparkSession spark;

    @BeforeAll
    public static void setup() {
        // System.setProperty("SPARK_HOME", "/opt/homebrew/Cellar/apache-spark/3.5.5");
        // System.setProperty("jdk.module.add-opens", "java.base/sun.nio.ch=ALL-UNNAMED");
        // System.setProperty("jdk.module.add-exports", "java.base/sun.nio.ch=ALL-UNNAMED");

         spark = SparkSession.builder()
            .appName("Java Spark SQL basic example")
            .config("spark.master", "local")
            .config("spark.driver.extraJavaOptions", "--add-opens java.base/sun.nio.ch=ALL-UNNAMED")
            .config("spark.executor.extraJavaOptions", "--add-opens java.base/sun.nio.ch=ALL-UNNAMED")
            .config("spark.driver.extraJavaOptions", "--add-exports java.base/sun.nio.ch=ALL-UNNAMED")
            .config("spark.executor.extraJavaOptions", "--add-exports java.base/sun.nio.ch=ALL-UNNAMED")
            .getOrCreate();

        // spark = SparkSession.builder()
        //         .appName("Test")
        //         .master("local[*]")   // Set master for tests
        //         .getOrCreate();
    }

    @Test
    public void TestFilesCanBeRead() {
        assert (spark != null);
        System.out.println("Spark test session running.");
    }

    @AfterAll
    public static void tearDown() {
        if (spark != null) {
            spark.stop();
        }
    }
}
