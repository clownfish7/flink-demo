package com.clownfish7.flink.transform;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;

import java.time.LocalDateTime;

/**
 * classname TransformConnect
 * description TransformConnect 连接两条流
 * create 2021-12-23 11:02
 */
public class TransformConnect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<TransformKeyByRollingAggregation.User> userStream = env.fromElements(
                new TransformKeyByRollingAggregation.User(LocalDateTime.now().minusDays(1), "user1", 18),
                new TransformKeyByRollingAggregation.User(LocalDateTime.now().minusDays(2), "user2", 19),
                new TransformKeyByRollingAggregation.User(LocalDateTime.now().minusDays(9), "user3", 20)
        );

        DataStreamSource<String> strStream = env.fromElements("str1", "str2");

        ConnectedStreams<TransformKeyByRollingAggregation.User, String> connect = userStream
                .connect(strStream);

        connect.map(new CoMapFunction<TransformKeyByRollingAggregation.User, String, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> map1(TransformKeyByRollingAggregation.User value) throws Exception {
                        return new Tuple2<>("map1", value.getName());
                    }

                    @Override
                    public Tuple2<String, String> map2(String value) throws Exception {
                        return new Tuple2<>("map2", value);
                    }
                })
                .print();

        // 同 Map
        connect.flatMap(new CoFlatMapFunction<TransformKeyByRollingAggregation.User, String, Object>() {
            @Override
            public void flatMap1(TransformKeyByRollingAggregation.User value, Collector<Object> out) throws Exception {

            }

            @Override
            public void flatMap2(String value, Collector<Object> out) throws Exception {

            }
        });

        env.execute();
    }
}
