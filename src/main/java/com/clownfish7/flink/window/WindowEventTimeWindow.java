package com.clownfish7.flink.window;

import com.clownfish7.flink.transform.TransformKeyByRollingAggregation;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.LocalDateTime;

/**
 * @author You
 * @create 2021-12-25 3:26 AM
 */
public class WindowEventTimeWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 过时方法
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        WindowedStream<String, Integer, TimeWindow> window = env.fromElements(
                        new TransformKeyByRollingAggregation.User(LocalDateTime.now().minusDays(1), "user1", 18),
                        new TransformKeyByRollingAggregation.User(LocalDateTime.now().minusDays(2), "user2", 19),
                        new TransformKeyByRollingAggregation.User(LocalDateTime.now().minusDays(3), "user3", 20),
                        new TransformKeyByRollingAggregation.User(LocalDateTime.now().minusDays(4), "user1", 25),
                        new TransformKeyByRollingAggregation.User(LocalDateTime.now().minusDays(5), "user2", 22),
                        new TransformKeyByRollingAggregation.User(LocalDateTime.now().minusDays(6), "user3", 23),
                        new TransformKeyByRollingAggregation.User(LocalDateTime.now().minusDays(7), "user1", 18),
                        new TransformKeyByRollingAggregation.User(LocalDateTime.now().minusDays(8), "user2", 19),
                        new TransformKeyByRollingAggregation.User(LocalDateTime.now().minusDays(9), "user3", 20))
                .keyBy((KeySelector<String, Integer>) String::length)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)));



        env.execute();
    }
}
