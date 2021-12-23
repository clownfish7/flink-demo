package com.clownfish7.flink.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * classname SinkKafka
 * description sink 2 kafka
 * create 2021-12-23 19:12
 */
public class SinkKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> stream = env.fromElements("stream1");

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.0.24:9092");

        FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<>("flink", new SimpleStringSchema(), properties);

        stream.addSink(myProducer);

        env.execute();
    }
}
