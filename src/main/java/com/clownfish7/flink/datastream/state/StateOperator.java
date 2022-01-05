package com.clownfish7.flink.datastream.state;

import com.clownfish7.flink.pojo.User;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;

/**
 * @author You
 * @create 2022-01-02 7:50 AM
 */
public class StateOperator {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行
        env.setParallelism(2);

        env
                .fromElements(
                        new User(LocalDateTime.now().minusDays(1), "user1", 18),
                        new User(LocalDateTime.now().minusDays(2), "user2", 19),
                        new User(LocalDateTime.now().minusDays(3), "user3", 20),
                        new User(LocalDateTime.now().minusDays(4), "user1", 25),
                        new User(LocalDateTime.now().minusDays(5), "user2", 22),
                        new User(LocalDateTime.now().minusDays(6), "user3", 23),
                        new User(LocalDateTime.now().minusDays(7), "user1", 18),
                        new User(LocalDateTime.now().minusDays(8), "user2", 19),
                        new User(LocalDateTime.now().minusDays(9), "user3", 20)
                )
                .map(new MyMapFunction())
                .print();


        env.execute("state operatorState");
    }

    // 过时
    public static class MyMapFunction implements MapFunction<User, Integer>, ListCheckpointed<Integer> {

        private Integer count = 0;

        @Override
        public Integer map(User user) throws Exception {
            this.count++;
            return count;
        }

        @Override
        public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
            return Collections.singletonList(count);
        }

        @Override
        public void restoreState(List<Integer> state) throws Exception {
            state.forEach(c -> count += c);
        }
    }
}
