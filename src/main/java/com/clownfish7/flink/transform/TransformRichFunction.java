package com.clownfish7.flink.transform;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * classname TransformRichFunction
 * description 富函数
 * create 2021-12-23 13:43
 * @author clownfish7
 */
public class TransformRichFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStreamSource<String> inputStream = env.fromElements("aa", "vv", "asf", "asff", "hello", "clownfish7");

        inputStream.map(new MyRichMapper()).print();

        env.execute("map");
    }

    public static class MyRichMapper extends RichMapFunction<String, Integer> {

        @Override
        public void open(Configuration parameters) throws Exception {
            // init 如连接数据库
            System.out.println("open");
            super.open(parameters);
        }

        @Override
        public Integer map(String value) throws Exception {
            System.out.println("-- " + getRuntimeContext().getIndexOfThisSubtask());
            return value.length();
        }

        @Override
        public void close() throws Exception {
            // 处理收尾，资源回收
            System.out.println("close");
            super.close();
        }
    }
}
