package com.clownfish7.flink.pojo;

import java.util.Date;

/**
 * @author You
 * @create 2022-01-02 9:23 AM
 */
public class Sensor {
    private Integer id;
    private Date time;
    private Double temp;

    public Sensor() {
    }

    public Sensor(Integer id, Date time, Double temp) {
        this.id = id;
        this.time = time;
        this.temp = temp;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Date getTime() {
        return time;
    }

    public void setTime(Date time) {
        this.time = time;
    }

    public Double getTemp() {
        return temp;
    }

    public void setTemp(Double temp) {
        this.temp = temp;
    }

    @Override
    public String toString() {
        return "Sensor{" +
                "id=" + id +
                ", time=" + time +
                ", temp=" + temp +
                '}';
    }
}
