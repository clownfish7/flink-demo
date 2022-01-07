package com.clownfish7.flink.tableapi.connectors;

import org.apache.flink.connector.print.table.PrintConnectorOptions;
import org.apache.flink.table.api.*;
import org.apache.flink.types.Row;

/**
 * classname PrintConnector
 * description 控制台连接器
 * create 2022-01-05 15:53
 */
public class PrintConnector {
    public static void main(String[] args) {
        TableEnvironment env = TableEnvironment.create(EnvironmentSettings.newInstance().inStreamingMode().build());

        env.createTemporaryTable("printTable", TableDescriptor.forConnector("print")
                .schema(Schema.newBuilder()
                        .column("id", DataTypes.INT())
//                        .column("time", DataTypes.DATE())
                        .column("temp", DataTypes.DOUBLE())
                        .build())
                // 配置一个标识符作为输出数据的前缀。
                .option(PrintConnectorOptions.PRINT_IDENTIFIER, "identifier")
                // 如果 format 需要打印为标准错误而不是标准输出，则为 True
                .option(PrintConnectorOptions.STANDARD_ERROR, false)
                // 为 Print sink operator 定义并行度。默认情况下，并行度由框架决定，和链在一起的上游 operator 一致
                .option("sink.parallelism", "1")
                .build());

        Table dataTable = env.fromValues(
                Row.of(1, 1.0)
        );

        dataTable.executeInsert("printTable");

    }
}
