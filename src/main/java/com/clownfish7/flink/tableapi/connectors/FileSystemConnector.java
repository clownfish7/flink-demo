package com.clownfish7.flink.tableapi.connectors;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * classname FileSystemConnector
 * description TODO
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
                "'path' = 'file://d:/flink-connector-fs.txt'," +
                "'connector' = 'filesystem'," +
                ")";
        env.executeSql("");
    }
}
