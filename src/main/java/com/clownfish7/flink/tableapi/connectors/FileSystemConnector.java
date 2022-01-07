package com.clownfish7.flink.tableapi.connectors;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;

/**
 * classname FileSystemConnector
 * description 文件系统连接器
 * create 2022-01-05 16:09
 */
public class FileSystemConnector {
    public static void main(String[] args) {
        TableEnvironment env = TableEnvironment.create(EnvironmentSettings.newInstance().build());

        String fsSql = "CREATE TABLE fs_table (" +
                "user_id STRING," +
                "order_amount DOUBLE," +
                "dt STRING," +
                "`hour` String" +
                ") PARTITIONED BY (dt,`hour`) WITH (" +
                "'connector' = 'filesystem'," +
                "'path' = 'file:///d:/aaaa'," +
                "'format' = 'csv'" +
                ")";
        env.executeSql(fsSql);

        env.fromValues(
                Row.of("id1", 10D, "20210106", "15"),
                Row.of("id1", 10D, "20210106", "15"),
                Row.of("id1", 10D, "20210107", "15"),
                Row.of("id1", 10D, "20210106", "14"),
                Row.of("id1", 10D, "20210106", "15")
        ).executeInsert("fs_table");
    }
}
