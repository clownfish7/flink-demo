package com.clownfish7.flink.cdc;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * classname CDC_Mysql_Code
 * description TODO
 * create 2022-04-29 9:45
 */
public class CDC_Mysql_Code {

    public static final String HOSTNAME = "192.168.0.24";
    public static final int PORT = 3306;
    public static final String DATABASE_LIST = "ponysafetyplus";
    public static final String TABLE_LIST = "ponysafetyplus.sys_log";
    public static final String USERNAME = "username";
    public static final String PASSWORD = "passaword";

    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
//        properties.setProperty("serverTimezone", "Asia/Shanghai");
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(HOSTNAME)
                .port(PORT)
                // set captured database, If you need to synchronize the whole database, Please set tableList to ".*".
                .databaseList(DATABASE_LIST)
                // set captured table
                .tableList(TABLE_LIST)
                .username(USERNAME)
                .password(PASSWORD)
                .startupOptions(StartupOptions.initial())
//                .serverTimeZone("Asia/Shanghai")
                .jdbcProperties(properties)
                // converts SourceRecord to JSON String
//                .deserializer(new JsonDebeziumDeserializationSchema())
                .deserializer(new StringDebeziumDeserializationSchema())
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // enable checkpoint
        env.enableCheckpointing(3000);

        env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                // set 4 parallel source tasks
                .setParallelism(4)
                // use parallelism 1 for sink to keep message ordering
                .print().setParallelism(1);

        env.execute("Print MySQL Snapshot + Binlog");
    }
}
