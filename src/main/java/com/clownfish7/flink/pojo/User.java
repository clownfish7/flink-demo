package com.clownfish7.flink.pojo;

import java.time.LocalDateTime;

/**
 * classname User
 * description TODO
 * create 2021-12-30 20:26
 */
public class User {
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
