package com.clownfish7.flink.tableapi.sql.queries;

/**
 * classname OverAggregation
 * description Over聚合
 * create 2022-01-10 14:45
 */
public class OverAggregation {
    /**
     * Over
     *
     * SELECT
     *   agg_func(agg_col) OVER (
     *     [PARTITION BY col1[, col2, ...]]
     *     ORDER BY time_col
     *     range_definition),
     *   ...
     * FROM ...
     */

    /**
     * 排序方式#
     * OVER窗口是在有序的行序列上定义的。由于表没有固有的顺序，因此该子句是必需的。对于流式查询，Flink 目前仅支持使用升序时间属性顺序定义的窗口。不支持其他排序
     *
     * 分区依据#
     * OVER窗口可以在分区表上定义。在存在子句的情况下，仅在其分区的行上为每个输入行计算聚合。PARTITION BY
     *
     * 范围定义#
     * 在 ORDER BY 列的值上定义间隔，在 Flink 情况下，该值始终是时间属性。以下 RANGE 间隔定义时间属性最多比当前行小 30 分钟的所有行都包含在聚合中。
     * RANGE BETWEEN INTERVAL '30' MINUTE PRECEDING AND CURRENT ROW
     *
     *
     * 行间隔#
     * 间隔是基于计数的间隔，它准确定义聚合中包含的行数。以下间隔定义当前行和当前行前面的 10 行（因此总共 11 行）包含在聚合中。
     * ROWS BETWEEN 10 PRECEDING AND CURRENT ROW
     *
     * 该子句可用于定义子句外部的窗口。它可以使查询更具可读性，还允许我们对多个聚合重用窗口定义。
     * SELECT order_id, order_time, amount,
     *      SUM(amount) OVER w as sum_amount,
     *      AVG(amount) OVER w as avg_amount
     * FROM Orders
     * WINDOW w AS (
     *      PARTITION BY product,
     *      ORDER BY order_time,
     *      RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW
     * )
     */
}
