package com.clownfish7.flink.cep;

import com.clownfish7.flink.cep.pojo.Event;
import com.clownfish7.flink.cep.pojo.SubEvent;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

/**
 * classname Case
 * description TODO
 * create 2022-01-10 15:46
 */
public class Case {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Event> input = env.fromElements();


        Pattern<Event, ?> pattern = Pattern
                .<Event>begin("start")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) throws Exception {
                        return event.getId() == 42;
                    }
                })
                .next("middle")
                .subtype(SubEvent.class)
                .where(new SimpleCondition<SubEvent>() {
                    @Override
                    public boolean filter(SubEvent value) throws Exception {
                        return false;
                    }
                })
                .followedBy("end")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event value) throws Exception {
                        return false;
                    }
                });


        PatternStream<Event> patternStream = CEP.pattern(input, pattern);


        patternStream.process(new PatternProcessFunction<Event, Object>() {
            @Override
            public void processMatch(Map<String, List<Event>> match, Context ctx, Collector<Object> out) throws Exception {
//                out.collect(createAlertFrom(pattern));
            }
        });

//        --------------------------------------------------------------------------------------------------------------
        // 单个模式

        Pattern<Object, Object> start = Pattern.begin("start");

        // 期望出现 4 次
        start.times(4);
        // 期望出现 0 次或 4 次
        start.times(4).optional();
        // 期望出现 2、3 或 4 次
        start.times(2, 4);
        // 期望出现 2、3 或 4 次，并且尽可能的多
        start.times(2, 4).greedy();
        // 期望出现 0、2、3 或 4 次
        start.times(2, 4).optional();
        // 期望出现 0、2、3 或 4 次，并且尽可能的多
        start.times(2, 4).optional().greedy();

        // 期望出现 1 到多次
        start.oneOrMore();
        start.oneOrMore().greedy();
        // 期望出现 0 到多次
        start.oneOrMore().optional();
        start.oneOrMore().optional().greedy();

        // 期望出现2到多次
        start.timesOrMore(2);
        // 期望出现2到多次，并且尽可能的重复次数多
        start.timesOrMore(2).greedy();
        // 期望出现0、2或多次
        start.timesOrMore(2).optional();
        // 期望出现0、2或多次，并且尽可能的重复次数多
        start.timesOrMore(2).optional().greedy();


        // 条件
        Pattern<Object, SubEvent> middle = start.oneOrMore()
                .subtype(SubEvent.class)
                .where(new IterativeCondition<SubEvent>() {
                    @Override
                    public boolean filter(SubEvent value, Context<SubEvent> ctx) throws Exception {
                        if (!value.getName().startsWith("foo")) {
                            return false;
                        }

                        double sum = value.getPrice();

                        // 调用ctx.getEventsForPattern(...)可以获得所有前面已经接受作为可能匹配的事件。
                        // 调用这个操作的代价可能很小也可能很大，所以在实现你的条件时，尽量少使用它。
                        for (Event event : ctx.getEventsForPattern("middle")) {
                            sum += event.getPrice();
                        }
                        return Double.compare(sum, 5.0) < 0;
                    }
                });


        // next();          指定严格连续
        // followedBy();    指定松散连续
        // followedByAny(); 指定不确定的松散连续

        // 严格连续
        // Pattern<Event, ?> strict = start.next("middle").where(...);

        // 松散连续
        // Pattern<Event, ?> relaxed = start.followedBy("middle").where(...);

        // 不确定的松散连续
        // Pattern<Event, ?> nonDetermin = start.followedByAny("middle").where(...);

        // 严格连续的NOT模式
        // Pattern<Event, ?> strictNot = start.notNext("not").where(...);

        // 松散连续的NOT模式
        // Pattern<Event, ?> relaxedNot = start.notFollowedBy("not").where(...);

        // 一个模式序列"a b+ c"（"a"后面跟着一个或者多个（不确定连续的）"b"，然后跟着一个"c"） 输入为"a"，"b1"，"d1"，"b2"，"d2"，"b3"，"c"，输出结果如下：
        // 严格连续: {a b3 c} – "b1"之后的"d1"导致"b1"被丢弃，同样"b2"因为"d2"被丢弃。
        //
        // 松散连续: {a b1 c}，{a b1 b2 c}，{a b1 b2 b3 c}，{a b2 c}，{a b2 b3 c}，{a b3 c} - "d"都被忽略了。
        //
        // 不确定松散连续: {a b1 c}，{a b1 b2 c}，{a b1 b3 c}，{a b1 b2 b3 c}，{a b2 c}，{a b2 b3 c}，{a b3 c} - 注意{a b1 b3 c}，这是因为"b"之间是不确定松散连续产生的。


    }
}
