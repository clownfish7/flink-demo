package com.clownfish7.flink.cep.pojo;

import java.util.Date;

/**
 * classname Event
 * description TODO
 * create 2022-01-13 10:18
 */
public class Event {

    private Integer id;
    private String type;
    private Date time;
    private Double price;

    public Event() {
    }

    public Event(Integer id, String type, Date time) {
        this.id = id;
        this.type = type;
        this.time = time;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Date getTime() {
        return time;
    }

    public void setTime(Date time) {
        this.time = time;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return "Event{" +
                "id='" + id + '\'' +
                ", type='" + type + '\'' +
                ", time=" + time +
                '}';
    }
}
