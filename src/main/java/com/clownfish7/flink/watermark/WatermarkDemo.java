package com.clownfish7.flink.watermark;

import com.clownfish7.flink.transform.TransformKeyByRollingAggregation;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.time.ZoneOffset;

/**
 * classname WatermarkDemo
 * description 水位线
 * create 2021-12-27 10:55
 */
public class WatermarkDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> source = env.fromElements("");

        // 处理升序策略
        WatermarkStrategy<TransformKeyByRollingAggregation.User> strategy1 = WatermarkStrategy
                .<TransformKeyByRollingAggregation.User>forMonotonousTimestamps()
                .withTimestampAssigner(
                        (SerializableTimestampAssigner<TransformKeyByRollingAggregation.User>)
                                (element, recordTimestamp) -> element.getLocalDateTime().toInstant(ZoneOffset.UTC).toEpochMilli()
                );

        // 处理乱序策略
        WatermarkStrategy<TransformKeyByRollingAggregation.User> strategy2 = WatermarkStrategy
                .<TransformKeyByRollingAggregation.User>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((TimestampAssignerSupplier<TransformKeyByRollingAggregation.User>) context -> null)
                // 指定 eventTime field
                .withTimestampAssigner((SerializableTimestampAssigner<TransformKeyByRollingAggregation.User>)
                        (element, recordTimestamp) -> element.getLocalDateTime().toInstant(ZoneOffset.UTC).toEpochMilli());

        // 自定义生成策略
        WatermarkStrategy<Object> strategy3 = WatermarkStrategy.forGenerator(
                (WatermarkGeneratorSupplier<Object>) context -> new WatermarkGenerator() {
                    @Override
                    public void onEvent(Object event, long eventTimestamp, WatermarkOutput output) {

                    }

                    @Override
                    public void onPeriodicEmit(WatermarkOutput output) {

                    }
                });

        source.assignTimestampsAndWatermarks(
                WatermarkStrategy.forMonotonousTimestamps()
        );

    }
}
