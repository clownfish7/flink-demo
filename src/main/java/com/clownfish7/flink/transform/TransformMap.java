package com.clownfish7.flink.transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * classname TransformMap
 * description Map
 * create 2021-12-22 11:14
 */
public class TransformMap {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> inputStream = env.fromElements("aa", "vv", "asf", "asff", "hello", "clownfish7");

        inputStream.map((MapFunction<String, Integer>) String::length).print();

        env.execute("map");
    }
}
