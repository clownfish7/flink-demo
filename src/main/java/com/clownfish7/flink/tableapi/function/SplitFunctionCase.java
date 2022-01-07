package com.clownfish7.flink.tableapi.function;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.*;

/**
 * classname SplitFunctionCase
 * description TODO
 * create 2022-01-06 20:36
 */
public class SplitFunctionCase {
    public static void main(String[] args) {
        TableEnvironment env = TableEnvironment.create(EnvironmentSettings.newInstance().inStreamingMode().build());
        Table dataTable = env.fromValues(
                Row.of("asfafef"),
                Row.of("asfafef"),
                Row.of("id1v1")
        );

        // 在 Table API 里不经注册直接“内联”调用函数
        dataTable.joinLateral(call(SplitFunction.class, $("f0")))
                .select($("*"))
                .executeInsert(TableDescriptor.forConnector("print").build());

        dataTable.leftOuterJoinLateral(call(SplitFunction.class, $("f0")))
                .select($("*"))
                .executeInsert(TableDescriptor.forConnector("print").build());


        // 在 Table API 里重命名函数字段
        dataTable.leftOuterJoinLateral(call(SplitFunction.class, $("f0")).as("newWord", "newLength"))
                .select($("f0"), $("newWord"), $("newLength"))
                .executeInsert(TableDescriptor.forConnector("print").build());

        // 注册函数
        env.createTemporarySystemFunction("SplitFunction", SplitFunction.class);

        // 在 Table API 里调用注册好的函数
        dataTable
                .joinLateral(call("SplitFunction", $("f0")))
                .select($("f0"), $("word"), $("length"))
                .executeInsert(TableDescriptor.forConnector("print").build());
        dataTable
                .leftOuterJoinLateral(call("SplitFunction", $("f0")))
                .select($("f0"), $("word"), $("length"))
                .executeInsert(TableDescriptor.forConnector("print").build());

        // 在 SQL 里调用注册好的函数
        env.sqlQuery(
                "SELECT myField, word, length " +
                        "FROM MyTable, LATERAL TABLE(SplitFunction(myField))");
        env.sqlQuery(
                "SELECT myField, word, length " +
                        "FROM MyTable " +
                        "LEFT JOIN LATERAL TABLE(SplitFunction(myField)) ON TRUE");

        // 在 SQL 里重命名函数字段
        env.sqlQuery(
                "SELECT myField, newWord, newLength " +
                        "FROM MyTable " +
                        "LEFT JOIN LATERAL TABLE(SplitFunction(myField)) AS T(newWord, newLength) ON TRUE");
    }
}
