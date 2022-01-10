package com.clownfish7.flink.tableapi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.*;

/**
 * classname Operations
 * create 2022-01-05 11:22
 */
public class Operations {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        TableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // create
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();

        TableEnvironment tEnv = TableEnvironment.create(settings);

        // 1. From  // 和 SQL 查询的 FROM 子句类似。 执行一个注册过的表的扫描。
        // support: Batch Streaming
        tEnv.from("Orders");

        // 2. FromValues  // 和 SQL 查询中的 VALUES 子句类似。 基于提供的行生成一张内联表。 你可以使用 row(...) 表达式创建复合行：
        // support: Batch Streaming
        tEnv.fromValues(
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.DECIMAL(10, 2)),
                        DataTypes.FIELD("name", DataTypes.STRING())
                ),
                Row.of(1, "ABC"),
                Row.of(2L, "ABCDE")
        );

        // 3. Select
        // support: Batch Streaming
        Table orders = tableEnv.from("Orders");
        Table result = orders.select($("a"), $("c").as("d"));

        // 4. As
        // support: Batch Streaming
        result = orders.as("x, y, z, t");

        // 5. Where/Filter
        // support: Batch Streaming
        orders.where($("b").isEqual("red"));
        orders.filter($("b").isEqual("red"));

        // 6. AddColumns 执行字段添加操作。 如果所添加的字段已经存在，将抛出异常
        // support: Batch Streaming
        orders.addColumns(concat($("c"), "sunny"));

        // 7. AddOrReplaceColumns
        // support: Batch Streaming
        orders.addOrReplaceColumns(concat($("c"), "sunny").as("desc"));

        // 8. DropColumns
        // support: Batch Streaming
        orders.dropColumns($("b"), $("c"));

        // 9. RenameColumns
        // support: Batch Streaming
        orders.renameColumns($("b").as("b2"), $("c").as("c2"));

        // 10. Aggregations
        // 10.1 GroupBy Aggregation 和 SQL 的 GROUP BY 子句类似。 使用分组键对行进行分组，使用伴随的聚合算子来按照组进行聚合行。
        // support: Batch Streaming ResultUpdating
        orders.groupBy($("a")).select($("a"), $("b").sum().as("d"));

        // 10.2 GroupBy Window Aggregation 使用分组窗口结合单个或者多个分组键对表进行分组和聚合。
        // support: Batch Streaming
        orders
                // 定义窗口
                .window(Tumble.over(lit(5).minutes()).on($("rowtime")).as("w"))
                // 按窗口和键分组
                .groupBy($("a"), $("w"))
                // 访问窗口属性并聚合
                .select(
                        $("a"),
                        $("w").start(),
                        $("w").end(),
                        $("w").rowtime(),
                        $("b").sum().as("d")
                );


        // 10.3 Over Window Aggregation 和 SQL 的 OVER 子句类似。 更多细节详见 over windows section
        orders
                .window(Over
                        .partitionBy($("a"))
                        .orderBy($("rowtime"))
                        .preceding(UNBOUNDED_RANGE)
                        .following(CURRENT_RANGE)
                        .as("w"))
                .select(
                        $("a"),
                        $("b").avg().over($("w")),
                        $("b").max().over($("w")),
                        $("b").min().over($("w"))
                );


        // 10.4 Distinct Aggregation
        // 和 SQL DISTINCT 聚合子句类似，例如 COUNT(DISTINCT a)。 Distinct 聚合声明的聚合函数（内置或用户定义的）
        // 仅应用于互不相同的输入值。 Distinct 可以应用于 GroupBy Aggregation、GroupBy Window Aggregation 和 Over Window Aggregation。

        // 按属性分组后的的互异（互不相同、去重）聚合
        orders
                .groupBy($("a"))
                .select($("a"), $("b").sum().distinct().as("d"));
        // 按属性、时间窗口分组后的互异（互不相同、去重）聚合
        orders
                .window(Tumble.over(lit(5).minutes()).on($("rowtime")).as("w"))
                .groupBy($("a"), $("w"))
                .select($("a"), $("b").sum().distinct().as("d"));
        // over window 上的互异（互不相同、去重）聚合
        orders
                .window(Over
                        .partitionBy($("a"))
                        .orderBy($("rowtime"))
                        .preceding(UNBOUNDED_RANGE)
                        .as("w"))
                .select($("a"), $("b").avg().distinct().over($("w")),
                        $("b").max().over($("w")),
                        $("b").min().over($("w")));


        // 11. Distinct 和 SQL 的 DISTINCT 子句类似。 返回具有不同组合值的记录。
        // support: Batch Streaming ResultUpdating
        orders.distinct();

        // 12. Joins

        // 10.

        // 10.

        // 10.

        // 10.

        // -------------------------------------------------------------------------------------------------------------

        /**
         * Group window 聚合根据时间或行计数间隔将行分为有限组，并为每个分组进行一次聚合函数计算。对于批处理表，窗口是按时间间隔对记录进行分组的便捷方式
         * Table table = input
         *   .window([GroupWindow w].as("w"))   // 定义窗口并指定别名为 w
         *   .groupBy($("w"))                   // 以窗口 w 对表进行分组
         *   .select($("b").sum());             // 聚合
         *
         * // Tumbling Event-time Window
         * .window(Tumble.over(lit(10).minutes()).on($("rowtime")).as("w"));
         *
         * // Tumbling Processing-time Window (assuming a processing-time attribute "proctime")
         * .window(Tumble.over(lit(10).minutes()).on($("proctime")).as("w"));
         *
         * // Tumbling Row-count Window (assuming a processing-time attribute "proctime")
         * .window(Tumble.over(rowInterval(10)).on($("proctime")).as("w"));
         *
         *
         *
         * // Sliding Event-time Window
         * .window(Slide.over(lit(10).minutes()) .every(lit(5).minutes()) .on($("rowtime")) .as("w"));
         *
         * // Sliding Processing-time window (assuming a processing-time attribute "proctime")
         * .window(Slide.over(lit(10).minutes()) .every(lit(5).minutes()) .on($("proctime")) .as("w"));
         *
         * // Sliding Row-count window (assuming a processing-time attribute "proctime")
         * .window(Slide.over(rowInterval(10)).every(rowInterval(5)).on($("proctime")).as("w"));
         *
         *
         *
         * // Session Event-time Window
         * .window(Session.withGap(lit(10).minutes()).on($("rowtime")).as("w"));
         *
         * // Session Processing-time Window (assuming a processing-time attribute "proctime")
         * .window(Session.withGap(lit(10).minutes()).on($("proctime")).as("w"));
         */


        /**
         * Over window 聚合聚合来自在标准的 SQL（OVER 子句），可以在 SELECT 查询子句中定义。与在“GROUP BY”子句中
         * 指定的 group window 不同， over window 不会折叠行。相反，over window 聚合为每个输入行在其相邻行的范围内计算聚合
         *
         * Table table = input
         *   .window([OverWindow w].as("w"))                                        // define over window with alias w
         *   .select($("a"), $("b").sum().over($("w")), $("c").min().over($("w"))); // aggregate over the over window w
         */

        /**
         * // 无界的事件时间 over window（假定有一个叫“rowtime”的事件时间属性）
         * .window(Over.partitionBy($("a")).orderBy($("rowtime")).preceding(UNBOUNDED_RANGE).as("w"));
         *
         * // 无界的处理时间 over window（假定有一个叫“proctime”的处理时间属性）
         * .window(Over.partitionBy($("a")).orderBy("proctime").preceding(UNBOUNDED_RANGE).as("w"));
         *
         * // 无界的事件时间行数 over window（假定有一个叫“rowtime”的事件时间属性）
         * .window(Over.partitionBy($("a")).orderBy($("rowtime")).preceding(UNBOUNDED_ROW).as("w"));
         *
         * // 无界的处理时间行数 over window（假定有一个叫“proctime”的处理时间属性）
         * .window(Over.partitionBy($("a")).orderBy($("proctime")).preceding(UNBOUNDED_ROW).as("w"));
         *
         *
         *
         * // 有界的事件时间 over window（假定有一个叫“rowtime”的事件时间属性）
         * .window(Over.partitionBy($("a")).orderBy($("rowtime")).preceding(lit(1).minutes()).as("w"));
         *
         * // 有界的处理时间 over window（假定有一个叫“proctime”的处理时间属性）
         * .window(Over.partitionBy($("a")).orderBy($("proctime")).preceding(lit(1).minutes()).as("w"));
         *
         * // 有界的事件时间行数 over window（假定有一个叫“rowtime”的事件时间属性）
         * .window(Over.partitionBy($("a")).orderBy($("rowtime")).preceding(rowInterval(10)).as("w"));
         *
         * // 有界的处理时间行数 over window（假定有一个叫“proctime”的处理时间属性）
         * .window(Over.partitionBy($("a")).orderBy($("proctime")).preceding(rowInterval(10)).as("w"));
         */


    }
}
