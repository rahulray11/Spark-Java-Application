package com.spark.processor;

import com.spark.reader.DataReader; // Import the DataReader interface
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions; // Use functions directly

import java.util.HashMap; // Import HashMap
import java.util.Map;     // Import Map

/**
 * Processes data using configuration information, relying on a DataReader for input.
 */
public class DataProcessor {

    // Private final field for the DataReader dependency
    private final DataReader dataReader;

    /**
     * Constructor for dependency injection of the DataReader.
     *
     * @param dataReader An implementation of the DataReader interface.
     */
    public DataProcessor(DataReader dataReader) {
        this.dataReader = dataReader;
    }

    /**
     * Runs the main data processing logic.
     * Reads data using the injected DataReader, then joins and filters.
     *
     * Assumes the DataReader provides datasets with expected columns:
     * - Snowflake: "id" (int), "value" (string)
     * - Cassandra: "config_id" (int), "threshold" (int)
     *
     * @return A Dataset<Row> containing the filtered results.
     */
    public Dataset<Row> runProcessingLogic() {
        // Example parameters for data reading - replace with actual configuration/parameters
        Map<String, String> sfProperties = new HashMap<>();
        // sfProperties.put("sfUrl", "your_snowflake_url"); // Example property
        // sfProperties.put("sfUser", "your_user");        // Example property
        // sfProperties.put("sfPassword", "your_password"); // Example property
        // sfProperties.put("sfDatabase", "your_db");      // Example property
        // sfProperties.put("sfSchema", "your_schema");    // Example property
        // sfProperties.put("sfWarehouse", "your_wh");     // Example property

        String sfQuery = "SELECT id, value FROM source_table"; // Example query
        String cassKeyspace = "config_keyspace"; // Example keyspace
        String cassTable = "config_table";       // Example table

        // Read data using the DataReader instance
        Dataset<Row> inputData = dataReader.readFromSnowflake(sfProperties, sfQuery);
        Dataset<Row> configData = dataReader.readFromCassandra(cassKeyspace, cassTable);

        // Join the two datasets on the ID columns
        Dataset<Row> joinedData = inputData.join(
            configData,
            inputData.col("id").equalTo(configData.col("config_id"))
        );

        // Filter the joined data based on the length of 'value' and 'threshold'
        Dataset<Row> filteredData = joinedData.filter(
            functions.length(inputData.col("value")).gt(configData.col("threshold"))
        );

        // Return the filtered dataset
        return filteredData;
    }
}
