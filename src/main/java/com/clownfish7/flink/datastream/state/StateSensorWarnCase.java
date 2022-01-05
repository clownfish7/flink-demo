package com.clownfish7.flink.datastream.state;

import com.clownfish7.flink.pojo.Sensor;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Date;
import java.util.Optional;

/**
 * @author You
 * @create 2022-01-02 9:24 AM
 */
public class StateSensorWarnCase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行
        env.setParallelism(1);


        // 状态后端 过时API
        env.setStateBackend(new MemoryStateBackend());
        env.setStateBackend(new FsStateBackend("uri"));
        // 新API
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage(new JobManagerCheckpointStorage());
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs:///checkpoints");

        //                  check point
        // 开启 checkpoint 300ms
        env.enableCheckpointing(300);
        // 精准一次
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //
        env.getCheckpointConfig().setCheckpointStorage("");
        // 最大并发
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        // 超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        // 前一次 checkPoint 完成与后一次 checkPoint 开始之间的 最小时间间隔 100ms
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(100);
        // 允许 checkPoint 失败次数 默认为 0
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(0);

        //                  重启策略
        // 固定延迟重启
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000L));
        // 失败率重启
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.minutes(10), Time.minutes(1)));


        env
                .fromElements(
                        new Sensor(1, new Date(), 10.1),
                        new Sensor(2, new Date(), 10.1),
                        new Sensor(1, new Date(), 12.1),
                        new Sensor(2, new Date(), 30.1),
                        new Sensor(1, new Date(), 30.1),
                        new Sensor(2, new Date(), 50.1),
                        new Sensor(1, new Date(), 10.1),
                        new Sensor(2, new Date(), 10.1),
                        new Sensor(1, new Date(), 10.1),
                        new Sensor(2, new Date(), 11.1)
                )
                .keyBy(Sensor::getId)
                .flatMap(new MyFlatMapFunction())
                .print();


        env.execute();
    }

    public static class MyFlatMapFunction extends RichFlatMapFunction<Sensor, Tuple3<Integer, Double, Double>> {

        private ValueState<Double> lastTempState;

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("lastTemp", Double.class));
        }

        @Override
        public void flatMap(Sensor value, Collector<Tuple3<Integer, Double, Double>> out) throws Exception {
            // 获取温度值
            Double temp = lastTempState.value();

            // 温差判断
            Optional.ofNullable(temp).ifPresent(t -> {
                if (Math.abs(value.getTemp() - t) > 10.0) {
                    out.collect(Tuple3.of(value.getId(), t, value.getTemp()));
                }
            });

            // 更新状态
            lastTempState.update(value.getTemp());
        }

        @Override
        public void close() throws Exception {
            lastTempState.clear();
        }
    }

}
