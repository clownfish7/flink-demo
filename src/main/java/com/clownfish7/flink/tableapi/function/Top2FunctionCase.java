package com.clownfish7.flink.tableapi.function;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

import static org.apache.flink.table.api.Expressions.*;

/**
 * classname Top2FunctionCase
 * description TODO
 * create 2022-01-07 13:30
 */
public class Top2FunctionCase {
    public static void main(String[] args) {
        TableEnvironment env = TableEnvironment.create(EnvironmentSettings.newInstance().inStreamingMode().build());
        Table dataTable = env.fromValues(
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.STRING()),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("price", DataTypes.INT())
                ),
                row("id1", "Latte", 6),
                row("id1", "Latte", 8),
                row("id1", "Latte", 5),
                row("id1", "Latte", 20),
                row("id2", "Milk", 3)
        );

        env.createTemporaryView("userSource", dataTable);

        env.createTemporarySystemFunction("top2", Top2Function.class);


        dataTable.printSchema();

        dataTable
                .groupBy($("id"))
                .flatAggregate(call("top2", $("price")).as("price", "rank"))
                .select($("*"))
                .execute().print();
    }
}
