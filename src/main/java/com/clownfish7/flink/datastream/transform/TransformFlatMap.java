package com.clownfish7.flink.datastream.transform;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * classname TransformMap
 * description FLatMap
 * create 2021-12-22 11:14
 */
public class TransformFlatMap {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> inputStream = env.fromElements("aa-asf", "vv-af", "asf-sadg", "asff-sdg", "hello-flink", "clownfish7-cool");

        inputStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                Arrays.stream(s.split("-"))
                        .forEach(collector::collect);
            }
        }).print();

        env.execute("flatMap");
    }
}
