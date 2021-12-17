package com.clownfish7.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * classname WordCount
 * description 批处理-离线数据
 * create 2021-12-13 19:16
 */
public class WordCount {
    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String filePath = "src/main/resources/words.txt";
        DataSet<String> dataSource = env.readTextFile(filePath);

        AggregateOperator<Tuple2<String, Integer>> sum = dataSource
                .flatMap(new Func())
                // 按照位置分组
                .groupBy(0)
                // 按照位置求和
                .sum(1);

        sum.print();

    }

    public static class Func implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) {
            for (String word : s.split(" ")) {
                collector.collect(new Tuple2<>(word, 1));
            }
        }
    }

}
