package com.clownfish7.flink.datastream.watermark;

import com.clownfish7.flink.pojo.User;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * classname WatermarkDemo
 * description 水位线
 * create 2021-12-27 10:55
 */
public class WatermarkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<User> source = env.fromElements(
                new User(LocalDateTime.now().plusSeconds(1), "user1", 18),
                new User(LocalDateTime.now().plusSeconds(2), "user2", 19),
                new User(LocalDateTime.now().plusSeconds(3), "user3", 20),
                new User(LocalDateTime.now().minusSeconds(400), "user1", 25),
                new User(LocalDateTime.now().minusSeconds(400), "user1", 25),
                new User(LocalDateTime.now().minusSeconds(400), "user1", 25),
                new User(LocalDateTime.now().minusSeconds(400), "user1", 25),
                new User(LocalDateTime.now().minusSeconds(400), "user1", 25),
                new User(LocalDateTime.now().minusSeconds(400), "user1", 25),
                new User(LocalDateTime.now().minusSeconds(400), "user1", 25),
                new User(LocalDateTime.now().minusSeconds(400), "user1", 25),
                new User(LocalDateTime.now().minusSeconds(400), "user1", 25),
                new User(LocalDateTime.now().minusSeconds(1200), "user1", 25),
                new User(LocalDateTime.now().minusSeconds(400), "user1", 25),
                new User(LocalDateTime.now().minusSeconds(400), "user1", 25),
                new User(LocalDateTime.now().minusSeconds(400), "user1", 25),
                new User(LocalDateTime.now().minusSeconds(400), "user1", 25),
                new User(LocalDateTime.now().minusSeconds(400), "user1", 25),
                new User(LocalDateTime.now().plusSeconds(400), "user1", 25),
                new User(LocalDateTime.now().plusSeconds(400), "user1", 25),
                new User(LocalDateTime.now().plusSeconds(400), "user1", 25),
                new User(LocalDateTime.now().plusSeconds(400), "user1", 25),
                new User(LocalDateTime.now().plusSeconds(400), "user1", 25),
                new User(LocalDateTime.now().plusSeconds(400), "user1", 25),
                new User(LocalDateTime.now().plusSeconds(400), "user1", 25),
                new User(LocalDateTime.now().plusSeconds(400), "user1", 25),
                new User(LocalDateTime.now().plusSeconds(400), "user1", 25),
                new User(LocalDateTime.now().plusSeconds(400), "user1", 25),
                new User(LocalDateTime.now().plusSeconds(400), "user1", 25),
                new User(LocalDateTime.now().plusSeconds(400), "user1", 25),
                new User(LocalDateTime.now().plusSeconds(400), "user1", 25),
                new User(LocalDateTime.now().plusSeconds(400), "user1", 25),
                new User(LocalDateTime.now().plusSeconds(400), "user1", 25),
                new User(LocalDateTime.now().minusSeconds(400), "user1", 25),
                new User(LocalDateTime.now().minusSeconds(400), "user1", 25),
                new User(LocalDateTime.now().minusSeconds(400), "user1", 25),
                new User(LocalDateTime.now().minusSeconds(400), "user1", 25),
                new User(LocalDateTime.now().minusSeconds(400), "user1", 25),
                new User(LocalDateTime.now().minusSeconds(400), "user1", 25),
                new User(LocalDateTime.now().minusSeconds(400), "user1", 25),
                new User(LocalDateTime.now().minusSeconds(1200), "user1", 25),
                new User(LocalDateTime.now().minusSeconds(400), "user1", 25),
                new User(LocalDateTime.now().minusSeconds(400), "user1", 0),
                new User(LocalDateTime.now().plusSeconds(5), "user2", 22),
                new User(LocalDateTime.now().minusSeconds(1200), "user1", 25),
                new User(LocalDateTime.now().plusSeconds(6), "user3", 23),
                new User(LocalDateTime.now().plusSeconds(7), "user1", 11),
                new User(LocalDateTime.now().plusSeconds(8), "user2", 12),
                new User(LocalDateTime.now().plusSeconds(9), "user3", 13)

                ,
                new User(LocalDateTime.now().plusSeconds(10), "user1", 33),
                new User(LocalDateTime.now().plusSeconds(11), "user2", 34),
                new User(LocalDateTime.now().minusSeconds(1200), "user1", 25),
                new User(LocalDateTime.now().plusSeconds(12), "user3", 35)
                ,
                new User(LocalDateTime.now().plusSeconds(13), "user1", 36),
                new User(LocalDateTime.now().plusSeconds(14), "user2", 37),
                new User(LocalDateTime.now().plusSeconds(15), "user3", 38)
                ,
                new User(LocalDateTime.now().plusSeconds(7), "user1", 42),
                new User(LocalDateTime.now().minusSeconds(1200), "user1", 25),
                new User(LocalDateTime.now().plusSeconds(8), "user2", 121),
                new User(LocalDateTime.now().minusSeconds(1200), "user1", 25),
                new User(LocalDateTime.now().plusSeconds(9), "user3", 41)
        );

        // 处理升序策略
        WatermarkStrategy<User> strategy1 = WatermarkStrategy
                .<User>forMonotonousTimestamps()
                // 指定 eventTime field
                .withTimestampAssigner(
                        (SerializableTimestampAssigner<User>)
                                (element, recordTimestamp) -> element.getLocalDateTime().toInstant(ZoneOffset.UTC).toEpochMilli()
                );

        // 处理乱序策略
        WatermarkStrategy<User> strategy2 = WatermarkStrategy
                .<User>forBoundedOutOfOrderness(Duration.ofSeconds(16))
//                .withTimestampAssigner((TimestampAssignerSupplier<User>) context -> null)
                // 指定 eventTime field
                .withTimestampAssigner((SerializableTimestampAssigner<User>)
                        (element, recordTimestamp) -> element.getLocalDateTime().toInstant(ZoneOffset.UTC).toEpochMilli())
                // 处理空闲数据源
//                .withIdleness(Duration.ofSeconds(1))
                ;

        // 自定义生成策略
        WatermarkStrategy<Object> strategy3 = WatermarkStrategy.forGenerator(
                (WatermarkGeneratorSupplier<Object>) context -> new WatermarkGenerator() {
                    @Override // 每个元素都调用这个方法，用于想依赖每个元素生成一个水印，然后发射到下游(可选，就是看是否用output来收集水印)
                    public void onEvent(Object event, long eventTimestamp, WatermarkOutput output) {
                        Watermark watermark = new Watermark(1L);
                        output.emitWatermark(watermark);
                    }

                    @Override // 用于周期性生成水印的方法。这个水印的生成周期可以这样设置：env.getConfig().setAutoWatermarkInterval(5000L);
                    public void onPeriodicEmit(WatermarkOutput output) {
                        Watermark watermark = new Watermark(1L);
                        output.emitWatermark(watermark);
                    }
                });

        OutputTag<User> outputTag = new OutputTag<>("lateTag", TypeInformation.of(User.class));
        source
                .assignTimestampsAndWatermarks(strategy2)
                .keyBy(User::getName)
                .window(TumblingEventTimeWindows.of(Time.seconds(9)))
                .allowedLateness(Time.seconds(2))
                .sideOutputLateData(outputTag)
//                .window(SlidingEventTimeWindows.of(Time.seconds(9),Time.seconds(1)))
                .minBy("age")
                .getSideOutput(outputTag).print("late");
        env.execute();
    }
}
