package com.clownfish7.flink.cep.case_login;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * classname LoginFailDetectExample
 * description TODO
 * create 2022-03-23 11:13
 */
public class LoginFailDetectExamplePro {
    public static void main(String[] args) throws Exception {
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<LoginEvent> loginEventStream = env.fromElements(
                        new LoginEvent("user_1", "192.168.0.1", "fail", 2000L),
                        new LoginEvent("user_1", "192.168.0.2", "fail", 3000L),
                        new LoginEvent("user_2", "192.168.1.29", "fail", 4000L),
                        new LoginEvent("user_1", "171.56.23.10", "fail", 5000L),
                        new LoginEvent("user_2", "192.168.1.29", "fail", 7000L),
//                        new LoginEvent("user_2", "192.168.1.29", "fail", 6000L),
                        new LoginEvent("user_2", "192.168.1.29", "fail", 8000L),
                        new LoginEvent("user_2", "192.168.1.29", "success", 6000L)
                )
                .assignTimestampsAndWatermarks(WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent>() {
                            @Override
                            public long extractTimestamp(LoginEvent element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );

        // cep pattern
        Pattern<LoginEvent, LoginEvent> pattern = Pattern
                .<LoginEvent>begin("fail")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent event) throws Exception {
                        return "fail".equals(event.eventType);
                    }
                })
                // 等价于 严格三次
                .times(3).consecutive();

        // cep pattern stream
        PatternStream<LoginEvent> patternStream = CEP.pattern(loginEventStream.keyBy(e -> e.userId), pattern);

        patternStream.process(new PatternProcessFunction<LoginEvent, String>() {
                    @Override
                    public void processMatch(Map<String, List<LoginEvent>> match, Context ctx, Collector<String> out) throws Exception {
                        StringBuilder builder = new StringBuilder();
                        match.get("fail").forEach(e -> {
                            builder.append(e.timestamp).append("\n");
                        });
                        out.collect(builder.toString());
                    }
                })
                .print();
        env.execute();

    }
}
