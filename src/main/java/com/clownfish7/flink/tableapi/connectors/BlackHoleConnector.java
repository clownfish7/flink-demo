package com.clownfish7.flink.tableapi.connectors;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;

/**
 * classname BlackHoleConnector
 * description BlackHole 连接器允许接收所有输入记录，就像类 Unix 操作系统上的 /dev/null。
 * create 2022-01-06 11:27
 */
public class BlackHoleConnector {
    public static void main(String[] args) {
        TableEnvironment env = TableEnvironment.create(EnvironmentSettings.newInstance().inStreamingMode().build());

        env.executeSql("CREATE TABLE blackhole_table (\n" +
                "  f0 INT,\n" +
                "  f1 INT,\n" +
                "  f2 STRING,\n" +
                "  f3 DOUBLE\n" +
                ") WITH (\n" +
                "  'connector' = 'blackhole'\n" +
                ")");

        Table dataTable = env.fromValues(
                Row.of(1, 1, "str", 1.0D)
        );

        dataTable.executeInsert("blackhole_table");
    }
}
