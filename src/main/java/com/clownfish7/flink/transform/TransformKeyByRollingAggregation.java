package com.clownfish7.flink.transform;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.LocalDateTime;

/**
 * classname TransformKeyByRollingAggregation
 * description KeyByRollingAggregation Max
 * create 2021-12-22 16:13
 */
public class TransformKeyByRollingAggregation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.fromElements(
                        new User(LocalDateTime.now().minusDays(1), "user1", 18),
                        new User(LocalDateTime.now().minusDays(2), "user2", 19),
                        new User(LocalDateTime.now().minusDays(3), "user3", 20),
                        new User(LocalDateTime.now().minusDays(4), "user1", 25),
                        new User(LocalDateTime.now().minusDays(5), "user2", 22),
                        new User(LocalDateTime.now().minusDays(6), "user3", 23),
                        new User(LocalDateTime.now().minusDays(7), "user1", 18),
                        new User(LocalDateTime.now().minusDays(8), "user2", 19),
                        new User(LocalDateTime.now().minusDays(9), "user3", 20)
                ).keyBy(User::getName)
                // 仅该字段变
                .max("age")
                // 对应实体其余字段也会更新
//                .maxby("age")
                .print();

        env.execute();
    }

    public static class User {
        private LocalDateTime localDateTime;
        private String name;
        private Integer age;

        public User() {
        }

        public User(LocalDateTime localDateTime, String name, Integer age) {
            this.localDateTime = localDateTime;
            this.name = name;
            this.age = age;
        }

        public LocalDateTime getLocalDateTime() {
            return localDateTime;
        }

        public void setLocalDateTime(LocalDateTime localDateTime) {
            this.localDateTime = localDateTime;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Integer getAge() {
            return age;
        }

        public void setAge(Integer age) {
            this.age = age;
        }

        @Override
        public String toString() {
            return "User{" +
                    "localDateTime=" + localDateTime +
                    ", name='" + name + '\'' +
                    ", age=" + age +
                    '}';
        }
    }
}
