package com.clownfish7.flink.tableapi;

import org.apache.flink.connector.datagen.table.DataGenConnectorOptions;
import org.apache.flink.table.api.*;

/**
 * classname Example
 * description TODO
 * create 2022-01-05 14:43
 */
public class Example {
    public static void main(String[] args) {

        // Create a TableEnvironment for batch or streaming execution.
        // See the "Create a TableEnvironment" section for details.
        TableEnvironment tableEnv = TableEnvironment.create(
                EnvironmentSettings.newInstance()
                        .inStreamingMode()
                        .build()
        );


        // Create a source table
        tableEnv.createTemporaryTable("SourceTable", TableDescriptor.forConnector("datagen")
                .schema(Schema.newBuilder()
                        .column("f0", DataTypes.STRING())
                        .build())
                .option(DataGenConnectorOptions.ROWS_PER_SECOND, 100L)
                .build());


        // Create a sink table(using SQL DDL)
        tableEnv.executeSql("CREATE TEMPORARY TABLE SinkTable WITH ('connector' = 'blackhole') LIKE SourceTable");

        // Create a table object from a Table API query
        Table table2 = tableEnv.from("SourceTable");

        // Create a table object from a SQL query
        Table table3 = tableEnv.sqlQuery("SELECT * FROM SourceTable");

        // Emit a Table API result to a TableSink, same for SQL result;
        TableResult tableResult = table2.executeInsert("SinkTable");

//        tableResult.print();

    }
}
