package com.clownfish7.flink.tableapi;

import com.clownfish7.flink.pojo.Sensor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Date;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author You
 * @create 2022-01-03 6:37 PM
 */
public class Case {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStream<Sensor> dataStream = env.fromElements(
                new Sensor(1, new Date(), 10.9D),
                new Sensor(1, new Date(), 10.9D),
                new Sensor(1, new Date(), 10.9D),
                new Sensor(2, new Date(), 10.9D),
                new Sensor(2, new Date(), 10.9D),
                new Sensor(2, new Date(), 10.9D),
                new Sensor(2, new Date(), 10.9D)
        );

        // 基于流创建表
        Table dataTable = tableEnv.fromDataStream(dataStream);

        // select
        Table select = dataTable
                // 过时方法 select("id, temp").where("id = 1")
                .select($("id"), $("temp"));

        tableEnv.toAppendStream(select, Row.class).print("oldApi");
        tableEnv.toDataStream(select).print("newApi");


        // sql
        // 注册表先
        tableEnv.registerTable("sensor", dataTable);
        String sql = "select id, temp from sensor where id = 1";
        tableEnv.executeSql(sql).print();
        tableEnv.executeSql(sql).collect().forEachRemaining(System.out::println);
        Table resultTable = tableEnv.sqlQuery(sql);
        tableEnv.toDataStream(resultTable).print("sqlResult");

        env.execute();
    }
}
