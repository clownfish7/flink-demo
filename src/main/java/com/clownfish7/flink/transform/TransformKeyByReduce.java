package com.clownfish7.flink.transform;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.LocalDateTime;

/**
 * classname TransformKeyByReduce
 * description KeyByReduce
 * create 2021-12-22 16:40
 */
public class TransformKeyByReduce {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.fromElements(
                        new TransformKeyByRollingAggregation.User(LocalDateTime.now().minusDays(1), "user1", 18),
                        new TransformKeyByRollingAggregation.User(LocalDateTime.now().minusDays(2), "user2", 19),
                        new TransformKeyByRollingAggregation.User(LocalDateTime.now().minusDays(3), "user3", 20),
                        new TransformKeyByRollingAggregation.User(LocalDateTime.now().minusDays(4), "user1", 25),
                        new TransformKeyByRollingAggregation.User(LocalDateTime.now().minusDays(5), "user2", 22),
                        new TransformKeyByRollingAggregation.User(LocalDateTime.now().minusDays(6), "user3", 23),
                        new TransformKeyByRollingAggregation.User(LocalDateTime.now().minusDays(7), "user1", 18),
                        new TransformKeyByRollingAggregation.User(LocalDateTime.now().minusDays(8), "user2", 19),
                        new TransformKeyByRollingAggregation.User(LocalDateTime.now().minusDays(9), "user3", 20)
                )
                .keyBy(TransformKeyByRollingAggregation.User::getName)
                .reduce((ReduceFunction<TransformKeyByRollingAggregation.User>) (value1, value2) -> {
                    TransformKeyByRollingAggregation.User user = new TransformKeyByRollingAggregation.User();
                    user.setLocalDateTime(value2.getLocalDateTime());
                    user.setName(value1.getName());
                    user.setAge(Math.max(value1.getAge(), value2.getAge()));
                    return user;
                })
                .print();

        env.execute();
    }
}
