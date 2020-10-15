package com.dyzcs.utils;

/**
 * Created by Administrator on 2020/10/15.
 */
public class RanOpt<T> {
    private T value;
    private int weight;

    public RanOpt(T value, int weight) {
        this.value = value;
        this.weight = weight;
    }

    public T getValue() {
        return value;
    }

    public int getWeight() {
        return weight;
    }
}
