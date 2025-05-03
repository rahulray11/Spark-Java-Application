package com.spark.processor;

import com.holdenkarau.spark.testing.JavaDatasetSuiteBase;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import com.spark.reader.DataReader; // Import the interface
import org.apache.spark.sql.Dataset; // Import Dataset
import org.junit.Test; // Using JUnit 4 based on pom.xml
import org.junit.runner.RunWith; // Import RunWith
import org.mockito.InjectMocks;   // Import InjectMocks
import org.mockito.Mock;          // Import Mock
import org.mockito.junit.MockitoJUnitRunner; // Import Mockito runner for JUnit 4

import java.util.Arrays;
import java.util.List;
import java.util.Map; // Import Map for mockito argument matching

import static org.junit.Assert.*; // Import common assertions
import static org.mockito.ArgumentMatchers.any; // Import Mockito matchers
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when; // Import Mockito static methods

/**
 * Unit tests for DataProcessor. Uses Mockito to mock DataReader.
 */
@RunWith(MockitoJUnitRunner.class) // Use Mockito runner
public class DataProcessorTest extends JavaDatasetSuiteBase {

    // Mock the DataReader dependency
    @Mock
    private DataReader dataReaderMock;

    // Inject the mock into the DataProcessor instance
    @InjectMocks
    private DataProcessor dataProcessor;

    // --- Schemas remain the same ---
    private static final StructType INPUT_SCHEMA = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField("id", DataTypes.IntegerType, false), // Not nullable
            DataTypes.createStructField("value", DataTypes.StringType, true)  // Nullable
    });

    private static final StructType CONFIG_SCHEMA = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField("config_id", DataTypes.IntegerType, false), // Not nullable
            DataTypes.createStructField("threshold", DataTypes.IntegerType, true) // Nullable threshold
    });

    // --- Static sample data lists are removed ---

    @Test
    public void testProcessDataLogic() {
        // --- Test data creation will now happen inside the test method ---
        // Create sample data for the mock DataReader to return
        List<Row> testInputData = Arrays.asList(
                RowFactory.create(1, "short"),
                RowFactory.create(2, "medium_val"),
                RowFactory.create(3, "a_very_long_value"),
                RowFactory.create(5, null),
                RowFactory.create(6, "another_long")
        );
        Dataset<Row> mockInputDf = spark().createDataFrame(testInputData, INPUT_SCHEMA);

        List<Row> testConfigData = Arrays.asList(
                RowFactory.create(1, 8),
                RowFactory.create(2, 8),
                RowFactory.create(3, 15),
                RowFactory.create(5, 3),
                RowFactory.create(6, 10)
        );
        Dataset<Row> mockConfigDf = spark().createDataFrame(testConfigData, CONFIG_SCHEMA);

        // --- Configure the mock DataReader ---
        // Use any() for Map and anyString() for String arguments as we don't care
        // about the specific connection details/query in this unit test.
        when(dataReaderMock.readFromSnowflake(any(Map.class), anyString())).thenReturn(mockInputDf);
        when(dataReaderMock.readFromCassandra(anyString(), anyString())).thenReturn(mockConfigDf);

        // --- Call the method under test ---
        // dataProcessor instance is already created by Mockito via @InjectMocks
        Dataset<Row> actualResultDf = dataProcessor.runProcessingLogic();

        // --- Define expected output ---
        // Based on the test data provided to the mock:
        // id=1: len=5 !> 8 -> No
        // id=2: len=10 > 8 -> Yes
        // id=3: len=17 > 15 -> Yes
        // id=5: len=null !> 3 -> No
        // id=6: len=12 > 10 -> Yes
        List<Row> expectedData = Arrays.asList(
                RowFactory.create(2, "medium_val", 2, 8),
                RowFactory.create(3, "a_very_long_value", 3, 15),
                RowFactory.create(6, "another_long", 6, 10)
        );

        // Expected schema remains the same (schema of the joined+filtered data)
        StructType expectedSchema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("value", DataTypes.StringType, true),
                DataTypes.createStructField("config_id", DataTypes.IntegerType, false),
                DataTypes.createStructField("threshold", DataTypes.IntegerType, true)
        });

        Dataset<Row> expectedResultDf = spark().createDataFrame(expectedData, expectedSchema);

        // --- Assert ---
        assertDatasetEquals(expectedResultDf, actualResultDf);
    }
}
