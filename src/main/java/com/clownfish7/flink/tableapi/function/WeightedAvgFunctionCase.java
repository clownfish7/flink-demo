package com.clownfish7.flink.tableapi.function;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

import static org.apache.flink.table.api.Expressions.row;

/**
 * classname WeightedAvgFunctionCase
 * description TODO
 * create 2022-01-07 10:46
 */
public class WeightedAvgFunctionCase {
    public static void main(String[] args) {
        TableEnvironment env = TableEnvironment.create(EnvironmentSettings.newInstance().inStreamingMode().build());
        Table dataTable = env.fromValues(
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.STRING()),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("price", DataTypes.INT()),
                        DataTypes.FIELD("points", DataTypes.INT().notNull()),
                        DataTypes.FIELD("level", DataTypes.INT().notNull())
                ),
                row("id1", "Latte", 6, 80, 2),
                row("id1", "Latte", 6, 20, 3),
                row("id2", "Milk", 3, 2, 7),
                row("id3", "Breve", 5, 1, 4),
                row("id3", "Breev", 5, 1, 4),
                row("id4", "Mocha", 8, 2, 2),
                row("id5", "Tea", 4, 2, 2)
        );

        env.createTemporaryView("userSource", dataTable);

        env.createTemporarySystemFunction("wAvgFunc", WeightedAvgFunction.class);

        dataTable.printSchema();

        env.executeSql("select id, wAvgFunc(points, level) as avgPoints from userSource GROUP by id")
//                .collect().forEachRemaining(System.out::println)
                .print()
        ;

    }
}
