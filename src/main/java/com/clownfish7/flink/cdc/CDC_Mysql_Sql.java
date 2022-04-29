package com.clownfish7.flink.cdc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * classname CDC_Mysql_Sql
 * description TODO
 * create 2022-04-28 20:11
 */
public class CDC_Mysql_Sql {

    public static final String MYSQL_CDC = "mysql-cdc";
    public static final String HOSTNAME = "192.168.0.24";
    public static final String PORT = "3306";
    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";
    public static final String DATABASE_NAME = "ponysafetyplus";
    public static final String TABLE_NAME = "sys_log";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);
        env.getCheckpointConfig().setCheckpointInterval(3000);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String tableSql = "CREATE TABLE sysLog (\n" +
                "    id STRING,\n" +
                "    type INT,\n" +
                "    title STRING,\n" +
                "    create_time TIMESTAMP,\n" +
                "    PRIMARY KEY (id) NOT ENFORCED\n" +
                "  ) WITH (\n" +
                "    'connector' = '" + MYSQL_CDC + "',\n" +
                "    'server-time-zone' = 'Asia/Shanghai',\n" +
                "    'hostname' = '" + HOSTNAME + "',\n" +
                "    'port' = '" + PORT + "',\n" +
                "    'username' = '" + USERNAME + "',\n" +
                "    'password' = '" + PASSWORD + "',\n" +
                "    'database-name' = '" + DATABASE_NAME + "',\n" +
                "    'table-name' = '" + TABLE_NAME + "'\n" +
                "  )";

        tableEnv.executeSql(tableSql);
        tableEnv.executeSql("SELECT * FROM sysLog").print();

    }
}
