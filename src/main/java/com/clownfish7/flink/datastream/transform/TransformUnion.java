package com.clownfish7.flink.datastream.transform;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * classname TransformUnion
 * description union 连接多条流，必须是同类型
 * create 2021-12-23 11:21
 */
public class TransformUnion {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> stream1 = env.fromElements("stream1");
        DataStreamSource<String> stream2 = env.fromElements("stream2");
        DataStreamSource<String> stream3 = env.fromElements("stream3");

        stream3.union(stream1, stream2).print();

        env.execute();
    }
}
