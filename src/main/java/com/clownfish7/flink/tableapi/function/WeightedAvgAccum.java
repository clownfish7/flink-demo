package com.clownfish7.flink.tableapi.function;

/**
 * classname WeightedAvgAccum
 * description TODO
 * create 2022-01-07 10:33
 */
public class WeightedAvgAccum {
    private int sum = 0;
    private int count = 0;

    public WeightedAvgAccum() {
    }

    public WeightedAvgAccum(int sum, int count) {
        this.sum = sum;
        this.count = count;
    }

    public int getSum() {
        return sum;
    }

    public void setSum(int sum) {
        this.sum = sum;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "WeightedAvgAccum{" +
                "sum=" + sum +
                ", count=" + count +
                '}';
    }
}
