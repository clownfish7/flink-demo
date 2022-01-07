package com.clownfish7.flink.tableapi.function;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * classname SplitFunction
 * description 表值函数
 * create 2022-01-06 20:34
 */
@FunctionHint(output = @DataTypeHint("Row<word String, length INT>"))
public class SplitFunction extends TableFunction<Row> {
    public void eval(String str) {
        collect(Row.of(str, str.length()));
    }
}
