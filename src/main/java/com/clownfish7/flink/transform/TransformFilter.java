package com.clownfish7.flink.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * classname TransformMap
 * description filter
 * create 2021-12-22 11:14
 */
public class TransformFilter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> inputStream = env.fromElements("aa-asf", "vv-af", "asf-sadg", "asff-sdg", "hello-flink", "clownfish7-cool");

        inputStream.filter((FilterFunction<String>) s -> s.length() > 3).print();

        env.execute("filter");
    }
}
