package com.clownfish7.flink.tableapi.connectors;

import org.apache.flink.connector.print.table.PrintConnectorOptions;
import org.apache.flink.table.api.*;

/**
 * classname DataGenConnector
 * description TODO
 * create 2022-01-05 16:48
 */
public class DataGenConnector {
    public static void main(String[] args) {
        TableEnvironment env = TableEnvironment.create(EnvironmentSettings.newInstance().build());

        env.executeSql("CREATE TABLE datagen (\n" +
                " f_sequence INT,\n" +
                " f_random INT,\n" +
                " f_random_str STRING,\n" +
                " ts AS localtimestamp,\n" +
                " WATERMARK FOR ts AS ts\n" +
                ") WITH (\n" +
                " 'connector' = 'datagen',\n" +
                "\n" +
                " -- optional options --\n" +
                "\n" +
                " 'rows-per-second'='5',\n" +
                "\n" +
                " 'fields.f_sequence.kind'='sequence',\n" +
                " 'fields.f_sequence.start'='1',\n" +
                " 'fields.f_sequence.end'='1000',\n" +
                "\n" +
                " 'fields.f_random.min'='1',\n" +
                " 'fields.f_random.max'='1000',\n" +
                "\n" +
                " 'fields.f_random_str.length'='10'\n" +
                ")").print();

        env.from("datagen").executeInsert(
                TableDescriptor.forConnector("print")
                        .schema(Schema.newBuilder()
                                .build())
                        // 配置一个标识符作为输出数据的前缀。
                        .option(PrintConnectorOptions.PRINT_IDENTIFIER, "identifier")
                        // 如果 format 需要打印为标准错误而不是标准输出，则为 True
                        .option(PrintConnectorOptions.STANDARD_ERROR, false)
                        // 为 Print sink operator 定义并行度。默认情况下，并行度由框架决定，和链在一起的上游 operator 一致
                        .option("sink.parallelism", "1")
                        .build());
    }
}
