package com.clownfish7.flink.datastream.state;

import com.clownfish7.flink.pojo.User;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.LocalDateTime;

/**
 * @author You
 * @create 2022-01-02 8:40 AM
 */
public class StateKeyed {
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
                .map(new StateOperator.MyMapFunction())
                .print();


        env.execute("state operatorState");
    }

    public static class MyFlatMapFunction extends RichFlatMapFunction<User, Integer> {

        private ValueState<Integer> keyCountState;
        private ListState<Integer> listState;
        private MapState<String, Integer> mapState;
        private ReducingState<User> reducingState;
        private AggregatingState<User, User> aggregatingState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            // param 3 设置默认值方法过时，建议判断 null
            keyCountState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("keyCountState", Integer.class, 0));
            listState = getRuntimeContext().getListState(new ListStateDescriptor<Integer>("listState", Integer.class));
            mapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Integer>("mapState", String.class, Integer.class));
            reducingState = getIterationRuntimeContext().getReducingState(new ReducingStateDescriptor<User>("reduceState", new ReduceFunction<User>() {
                @Override
                public User reduce(User value1, User value2) throws Exception {
                    // 在调用 reducingState.add(); 时会调用此 reduce Function
                    return null;
                }
            }, User.class));

        }

        @Override
        public void flatMap(User user, Collector<Integer> collector) throws Exception {
            // 操作各 state
            keyCountState.update(1);
        }

        @Override
        public void close() throws Exception {
            // 状态清理
            keyCountState.clear();
            reducingState.clear();
            super.close();
        }
    }

}
