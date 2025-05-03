package com.spark.reader;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import java.util.Map;

/**
 * Interface for reading data from different sources like Snowflake and Cassandra.
 */
public interface DataReader {

    /**
     * Reads data from Snowflake using the provided properties and query.
     *
     * @param properties A Map containing connection properties for Snowflake.
     * @param query The SQL query to execute on Snowflake.
     * @return A Dataset<Row> containing the data read from Snowflake.
     */
    Dataset<Row> readFromSnowflake(Map<String, String> properties, String query);

    /**
     * Reads data from a Cassandra table.
     *
     * @param keyspace The name of the keyspace in Cassandra.
     * @param table The name of the table within the keyspace.
     * @return A Dataset<Row> containing the data read from the Cassandra table.
     */
    Dataset<Row> readFromCassandra(String keyspace, String table);
}
