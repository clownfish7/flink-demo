package com.clownfish7.flink.datastream.window;

import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
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

/**
 * classname WindowTimeWindow
 * description 时间窗口
 * create 2021-12-24 13:18
 */
public class WindowTimeWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        WindowedStream<String, Integer, TimeWindow> window = env.socketTextStream("192.168.0.24", 9999)
                .keyBy((KeySelector<String, Integer>) String::length)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)));

        // 时间窗口 - 增量聚合
        window.aggregate(new AggregateFunction<String, Integer, Integer>() {
            @Override
            public Integer createAccumulator() {
                return 0;
            }

            @Override
            public Integer add(String value, Integer accumulator) {
                return accumulator + 1;
            }

            @Override
            public Integer getResult(Integer accumulator) {
                return accumulator;
            }

            @Override
            public Integer merge(Integer a, Integer b) {
                return a + b;
            }
        }).print();

        System.out.println("---------------------------------");

        // 时间窗口 - 全窗口聚合
        window.apply(new WindowFunction<String, Tuple3<Integer, Long, Integer>, Integer, TimeWindow>() {
            @Override
            public void apply(Integer key, TimeWindow window, Iterable<String> input, Collector<Tuple3<Integer, Long, Integer>> out) throws Exception {
                int size = IteratorUtils.toList(input.iterator()).size();
                long end = window.getEnd();
                out.collect(Tuple3.of(key, end, size));
            }
        }).print();

        // 多一个上下文可获取更多信息
        DataStream<Object> resultStream = window.process(new ProcessWindowFunction<String, Object, Integer, TimeWindow>() {
            @Override
            public void process(Integer integer, ProcessWindowFunction<String, Object, Integer, TimeWindow>.Context context, Iterable<String> elements, Collector<Object> out) throws Exception {

            }
        });

        // other api
        // 允许迟到数据
        window.allowedLateness(Time.seconds(1));
        // 其他迟到数据处理
        OutputTag<String> outputTag = new OutputTag<>("lateTag");
        window.sideOutputLateData(outputTag)
                .sum(0)
                .getSideOutput(outputTag)
                .print()

        ;

        env.execute();
    }
}
