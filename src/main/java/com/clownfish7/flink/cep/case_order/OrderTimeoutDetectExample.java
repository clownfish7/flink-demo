package com.clownfish7.flink.cep.case_order;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @author You
 * @create 2022-03-27 23:26
 */
public class OrderTimeoutDetectExample {
    public static void main(String[] args) throws Exception {
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<OrderEvent> orderEventStream = env.fromElements(
                        new OrderEvent("user_1", "order_1", "create", 1000L),
                        new OrderEvent("user_2", "order_2", "create", 2000L),
                        new OrderEvent("user_1", "order_1", "modify", 10 * 1000L),
                        new OrderEvent("user_1", "order_1", "pay", 60 * 1000L),
                        new OrderEvent("user_2", "order_3", "create", 10 * 60 * 1000L),
                        new OrderEvent("user_2", "order_3", "pay", 20 * 60 * 1000L)
                )
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderEvent>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderEvent>() {
                            @Override
                            public long extractTimestamp(OrderEvent element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));

        Pattern<OrderEvent, OrderEvent> pattern = Pattern.<OrderEvent>begin("create")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return value.eventType.equals("create");
                    }
                })
                .followedBy("pay")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return value.eventType.equals("pay");
                    }
                })
                .within(Time.minutes(15));

        PatternStream<OrderEvent> patternStream = CEP.pattern(orderEventStream.keyBy(e -> e.orderId), pattern);

        OutputTag<String> timeoutTag = new OutputTag<>("timeout", TypeInformation.of(String.class));

        SingleOutputStreamOperator<String> result = patternStream.select(timeoutTag,
                new PatternTimeoutFunction<OrderEvent, String>() {
                    @Override
                    public String timeout(Map<String, List<OrderEvent>> pattern, long timeoutTimestamp) throws Exception {
                        OrderEvent createEvent = pattern.get("create").get(0);
                        return "timeout " + createEvent.orderId + " " + createEvent.userId + " " + createEvent.timestamp;
                    }
                },
                new PatternSelectFunction<OrderEvent, String>() {
                    @Override
                    public String select(Map<String, List<OrderEvent>> pattern) throws Exception {
                        OrderEvent payEvent = pattern.get("pay").get(0);
                        return "paid " + payEvent.orderId + " " + payEvent.userId + " " + payEvent.timestamp;
                    }
                }
        );
        result.print();
        result.getSideOutput(timeoutTag).print();

        SingleOutputStreamOperator<String> resultProcess = patternStream.process(new OrderPayMatch());
        resultProcess.print("paid:");
        resultProcess.getSideOutput(timeoutTag).print("timeout:");

        env.execute();
    }

    public static class OrderPayMatch extends PatternProcessFunction<OrderEvent, String> implements TimedOutPartialMatchHandler<OrderEvent> {

        @Override
        public void processMatch(Map<String, List<OrderEvent>> match, Context ctx, Collector<String> out) throws Exception {
            OrderEvent payEvent = match.get("pay").get(0);
            out.collect("paid " + payEvent.orderId + " " + payEvent.userId + " " + payEvent.timestamp);
        }

        @Override
        public void processTimedOutMatch(Map<String, List<OrderEvent>> match, Context ctx) throws Exception {
            OutputTag<String> timeoutTag = new OutputTag<>("timeout", TypeInformation.of(String.class));
            OrderEvent createEvent = match.get("create").get(0);
            ctx.output(timeoutTag, "timeout " + createEvent.orderId + " " + createEvent.userId + " " + createEvent.timestamp);
        }
    }
}
