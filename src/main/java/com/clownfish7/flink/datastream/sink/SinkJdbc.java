package com.clownfish7.flink.datastream.sink;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * classname SinkJdbc
 * description sink 2 jdbc
 * create 2021-12-24 10:17
 */
public class SinkJdbc {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> stream = env.fromElements("stream1");

        stream.addSink(JdbcSink.sink(
                "select 1",
                (JdbcStatementBuilder<String>) (ps, s) -> {
                    ps.setInt(1, 1);
                    ps.setString(2, "t.title");
                    ps.setString(3, "t.author");
                    ps.setDouble(4, 4D);
                    ps.setInt(5, 5);
                }, new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("url")
                        .withUsername("root")
                        .withPassword("root")
                        .withDriverName("driverName")
                        .build())
        );

        env.execute();
    }
}
