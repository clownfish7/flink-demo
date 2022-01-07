package com.clownfish7.flink.tableapi.function;

import org.apache.flink.table.functions.ScalarFunction;

/**
 * classname SubstringFunction
 * description 标量函数
 * create 2022-01-06 11:42
 */
public class SubstringFunction extends ScalarFunction {
    public String eval(String s, Integer begin, Integer end) {
        return s.substring(begin, end);
    }
}
