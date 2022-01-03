package com.clownfish7.flink.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchemaBuilder;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;

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

        DataStream<String> stream = env.fromElements("stream1", "stream1", "stream1");

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.0.24:9092");

        // 过时方法
        FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<>("flink", new SimpleStringSchema(), properties);

        // 新方法
        KafkaSink<String> sink = KafkaSink.builder()
                .setBootstrapServers("192.168.0.24:9092")
                .setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setRecordSerializer(
                        new KafkaRecordSerializationSchemaBuilder()
                                .setTopic("flink")
                                .setKafkaKeySerializer(StringSerializer.class)
                                .setKafkaValueSerializer(StringSerializer.class)
                                .build())
                .build();

        stream.sinkTo(sink);

        stream.addSink(myProducer);

        env.execute();
    }
}
