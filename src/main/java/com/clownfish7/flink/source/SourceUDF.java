package com.clownfish7.flink.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * classname SourceUDF
 * description 自定义 Source
 * create 2021-12-21 17:03
 */
public class SourceUDF {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> dataStream = env.addSource(new MyUDFSource());

        dataStream.print("ss");

        env.execute("udf");
    }

    public static class MyUDFSource implements SourceFunction<String> {

        private boolean running = true;

        @Override
        public void run(SourceContext<String> sourceContext) throws Exception {
            while (running) {
                sourceContext.collect("hello , this generated by my udf source");
            }

        }

        @Override
        public void cancel() {
            this.running = false;
        }
    }
}
