package com.clownfish7.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * classname WindowWordCount
 * description 流处理-实时数据
 * create 2021-12-10 11:43
 */
public class WindowWordCount {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行
        env.setParallelism(2);

//        String filePath = "src/main/resources/words.txt";
//        DataStream<String> dataStream = env.readTextFile(filePath);
//        dataStream.flatMap(new Splitter()).keyBy(0).sum(1).print();

        // 获取参数工具
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host", "127.0.0.1");
        int port = parameterTool.getInt("port", 9999);

        env.socketTextStream(host, port)
                .flatMap(new Splitter())
                .keyBy(v -> v.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .sum(1)
                .print();

        env.execute("Window WordCount");

    }

    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) {
            for (String word : s.split(" ")) {
                collector.collect(new Tuple2<>(word, 1));
            }
        }
    }
}
