package com.clownfish7.flink.datastream.process;

import com.clownfish7.flink.pojo.User;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.LocalDateTime;

/**
 * @author You
 * @create 2022-01-02 3:55 PM
 */
public class KeyedProcess {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行
        env.setParallelism(1);

        env
                .fromElements(
                        new User(LocalDateTime.now().minusDays(1), "user1", 18),
                        new User(LocalDateTime.now().minusDays(2), "user2", 19),
                        new User(LocalDateTime.now().minusDays(3), "user3", 20),
                        new User(LocalDateTime.now().minusDays(4), "user1", 25),
                        new User(LocalDateTime.now().minusDays(5), "user2", 22),
                        new User(LocalDateTime.now().minusDays(6), "user3", 23),
                        new User(LocalDateTime.now().minusDays(7), "user1", 18),
                        new User(LocalDateTime.now().minusDays(8), "user2", 19),
                        new User(LocalDateTime.now().minusDays(9), "user3", 20)
                )
                .keyBy(User::getName)
                .process(new MyKeyedProcessFunction())
                .print();

        env.execute();
    }

    public static class MyKeyedProcessFunction extends KeyedProcessFunction<String, User, Integer> {

        @Override
        public void processElement(User value, KeyedProcessFunction<String, User, Integer>.Context ctx, Collector<Integer> out) throws Exception {
            out.collect(value.getAge());

            // context
            // 时间戳
            ctx.timestamp();
            //
            ctx.getCurrentKey();

//            ctx.output();

            ctx.timerService().currentProcessingTime();
            ctx.timerService().currentWatermark();
            // 绝对时间戳  1970 至 当前 时间戳       时间戳可存储state
            ctx.timerService().registerEventTimeTimer(ctx.timerService().currentProcessingTime() + 1000);
            ctx.timerService().registerProcessingTimeTimer(1L);

//            ctx.timerService().deleteEventTimeTimer(1L);
//            ctx.timerService().deleteEventTimeTimer(1L);
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, User, Integer>.OnTimerContext ctx, Collector<Integer> out) throws Exception {
            System.out.println("定时器触发: " + timestamp);
            ctx.getCurrentKey();
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
        }

        @Override
        public void close() throws Exception {
            super.close();
        }
    }
}
