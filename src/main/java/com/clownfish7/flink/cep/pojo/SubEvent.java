package com.clownfish7.flink.cep.pojo;

/**
 * classname SubEvent
 * description TODO
 * create 2022-01-13 10:23
 */
public class SubEvent extends Event {
    private String name;
    private Double price;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }
}
