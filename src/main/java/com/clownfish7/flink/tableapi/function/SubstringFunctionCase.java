package com.clownfish7.flink.tableapi.function;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * classname SubstringFunctionCase
 * description TODO
 * create 2022-01-06 11:44
 */
public class SubstringFunctionCase {
    public static void main(String[] args) {
        TableEnvironment env = TableEnvironment.create(EnvironmentSettings.newInstance().inStreamingMode().build());

        Table dataTable = env.fromValues(
                Row.of("id1", "v1"),
                Row.of("id1", "v1"),
                Row.of("id1", "v1"),
                Row.of("id1", "v1"),
                Row.of("id1", "v1"),
                Row.of("id1", "v1")
        );

        env.createTemporaryView("MyTable", dataTable);

        // 在 Table API 里不经注册直接“内联”调用函数
        dataTable.select(call(new SubstringFunction(), $("f0"), 1, 2))
                .executeInsert(TableDescriptor.forConnector("print").build());

        // 注册函数
        env.createTemporarySystemFunction("SubstringFunction", SubstringFunction.class);

        // 在 Table API 里调用注册好的函数
        dataTable.select(call("SubstringFunction", $("f0"), 1, 3))
                .executeInsert(TableDescriptor.forConnector("print").build());

        // 在 SQL 里调用注册好的函数
        env.sqlQuery("SELECT SubstringFunction(f0, 0, 3) FROM MyTable")
                .executeInsert(TableDescriptor.forConnector("print").build());

    }
}
