// package org.spark.test;

// import static org.junit.jupiter.api.Assertions.assertEquals;
// import static org.junit.jupiter.api.Assertions.assertFalse;
// import static org.junit.jupiter.api.Assertions.assertThrows;
// import static org.junit.jupiter.api.Assertions.assertTrue;

// import java.io.IOException;

// import org.junit.jupiter.api.Test;

// public class GenerateIndexTest {

//     public static String resourcePath = "src/test/resources/";
    
//     @Test
//     public void NoParameters() {        
//         try {
//             GenerateIndex.main(new String[0]);
//             // Should not make it here because will have error exit
//             assertFalse(true);
//         }
//         catch (IOException e) {
//             assertTrue(true);
//         }
//     }

//     @Test
//     public void TestBigFileDoesNotExist() {
//         String[] params = {"big.csv", "test01-small.csv", "out.csv"};
//         assertThrows(Exception.class, () -> GenerateIndex.main(params));
//     }
// }
