package com.clownfish7.flink.tableapi.sql.queries;

/**
 * classname WindowAggregation
 * description 窗口聚合
 * create 2022-01-10 14:20
 */
public class WindowAggregation {
    /**
     * GROUPING SETS
     *
     * SELECT supplier_id, rating, COUNT(*) AS total
     * FROM (VALUES
     *     ('supplier1', 'product1', 4),
     *     ('supplier1', 'product2', 3),
     *     ('supplier2', 'product3', 3),
     *     ('supplier2', 'product4', 4))
     * AS Products(supplier_id, product_id, rating)
     * GROUP BY GROUPING SETS ((supplier_id, rating), (supplier_id), ())
     *
     * Results:
     *
     * +-------------+--------+-------+
     * | supplier_id | rating | total |
     * +-------------+--------+-------+
     * |   supplier1 |      4 |     1 |
     * |   supplier1 | (NULL) |     2 |
     * |      (NULL) | (NULL) |     4 |
     * |   supplier1 |      3 |     1 |
     * |   supplier2 |      3 |     1 |
     * |   supplier2 | (NULL) |     2 |
     * |   supplier2 |      4 |     1 |
     * +-------------+--------+-------+
     */


    /**
     * ROLLUP
     *
     * SELECT supplier_id, rating, COUNT(*)
     * FROM (VALUES
     *     ('supplier1', 'product1', 4),
     *     ('supplier1', 'product2', 3),
     *     ('supplier2', 'product3', 3),
     *     ('supplier2', 'product4', 4))
     * AS Products(supplier_id, product_id, rating)
     * GROUP BY ROLLUP (supplier_id, rating)
     */


    /**
     * CUBE
     *
     * SELECT supplier_id, rating, product_id, COUNT(*)
     * FROM (VALUES
     *     ('supplier1', 'product1', 4),
     *     ('supplier1', 'product2', 3),
     *     ('supplier2', 'product3', 3),
     *     ('supplier2', 'product4', 4))
     * AS Products(supplier_id, product_id, rating)
     * GROUP BY CUBE (supplier_id, rating, product_id)
     *
     * SELECT supplier_id, rating, product_id, COUNT(*)
     * FROM (VALUES
     *     ('supplier1', 'product1', 4),
     *     ('supplier1', 'product2', 3),
     *     ('supplier2', 'product3', 3),
     *     ('supplier2', 'product4', 4))
     * AS Products(supplier_id, product_id, rating)
     * GROUP BY GROUPING SET (
     *     ( supplier_id, product_id, rating ),
     *     ( supplier_id, product_id         ),
     *     ( supplier_id,             rating ),
     *     ( supplier_id                     ),
     *     (              product_id, rating ),
     *     (              product_id         ),
     *     (                          rating ),
     *     (                                 )
     * )
     */


    /**
     * HAVING
     *
     * SELECT SUM(amount)
     * FROM Orders
     * GROUP BY users
     * HAVING SUM(amount) > 50
     */
}
