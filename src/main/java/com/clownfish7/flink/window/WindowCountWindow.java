package com.clownfish7.flink.window;

import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

/**
 * classname WindowCountWindow
 * description 计数窗口
 * create 2021-12-24 14:17
 */
public class WindowCountWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        WindowedStream<String, Integer, GlobalWindow> window = env.socketTextStream("192.168.0.24", 9999)
                .keyBy((KeySelector<String, Integer>) String::length)
                // 窗口长度为10，步长为2
                .countWindow(10, 2);

        // 增量聚合
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

        // 全窗口聚合
        window.apply(new WindowFunction<String, Tuple3<Integer, Long, Integer>, Integer, GlobalWindow>() {
            @Override
            public void apply(Integer key, GlobalWindow window, Iterable<String> input, Collector<Tuple3<Integer, Long, Integer>> out) throws Exception {
                int size = IteratorUtils.toList(input.iterator()).size();
                long end = window.maxTimestamp();
                out.collect(Tuple3.of(key, end, size));
            }
        }).print();

        // 多一个上下文可获取更多信息
        window.process(new ProcessWindowFunction<String, Object, Integer, GlobalWindow>() {
            @Override
            public void process(Integer integer, ProcessWindowFunction<String, Object, Integer, GlobalWindow>.Context context, Iterable<String> elements, Collector<Object> out) throws Exception {

            }
        }).print();

        env.execute();
    }
}
