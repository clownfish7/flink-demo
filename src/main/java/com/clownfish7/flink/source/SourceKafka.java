package com.clownfish7.flink.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaDeserializationSchemaWrapper;

import java.util.Arrays;
import java.util.Properties;

/**
 * classname SourceKafka
 * description kafka
 * create 2021-12-21 15:41
 */
public class SourceKafka {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.0.24:9092");
        properties.setProperty("group.id", "test");

        // 过时
        FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer<>(
                "flink", new SimpleStringSchema(), properties);
//        flinkKafkaConsumer.setStartFromEarliest();     // 尽可能从最早的记录开始
//        flinkKafkaConsumer.setStartFromLatest();       // 从最新的记录开始
//        flinkKafkaConsumer.setStartFromTimestamp(...); // 从指定的时间开始（毫秒）
        flinkKafkaConsumer.setStartFromGroupOffsets(); // 默认的方法

        // 新方法
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("192.168.0.24:9092")
                .setGroupId("MyGroup")
//                .setTopics(Arrays.asList("TOPIC1", "TOPIC2"))
                .setTopics(Arrays.asList("flink"))
                .setDeserializer(KafkaRecordDeserializationSchema.of(
                        new KafkaDeserializationSchemaWrapper(new SimpleStringSchema()))
                )
                .setStartingOffsets(OffsetsInitializer.earliest())
                .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafkaSourceName").print();

        DataStream<String> dataStream = env.addSource(flinkKafkaConsumer);

        dataStream.print();

        env.execute();
    }

}
