package com.clownfish7.flink.tableapi.function;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

/**
 * classname Top2Function
 * description 表聚合函数
 * create 2022-01-07 13:29
 */
public class Top2Function extends TableAggregateFunction<Tuple2<Integer, Integer>, Top2Accum> {


    @Override
    public Top2Accum createAccumulator() {
        Top2Accum top2Accum = new Top2Accum();
        top2Accum.first = Integer.MIN_VALUE;
        top2Accum.second = Integer.MIN_VALUE;

        top2Accum.oldFirst = Integer.MIN_VALUE;
        top2Accum.oldSecond = Integer.MIN_VALUE;
        return top2Accum;
    }

    public void accumulate(Top2Accum acc, Integer v) {
        if (v > acc.first) {
            acc.second = acc.first;
            acc.first = v;
        } else if (v > acc.second) {
            acc.second = v;
        }
    }

//    public void retract(ACC accumulator, [user defined inputs]); // OPTIONAL

    // 在许多批式聚合和以及流式会话和滑动窗口聚合中是必须要实现的
    public void merge(Top2Accum acc, Iterable<Top2Accum> it) {
        for (Top2Accum otherAcc : it) {
            accumulate(acc, otherAcc.first);
            accumulate(acc, otherAcc.second);
        }
    }


    /**
     * emitValue 方法会发送所有 accumulator 给出的结果。拿 TopN 来说，emitValue 每次都会发送所有的最大的 n 个值。
     * 这在流式任务中可能会有一些性能问题。为了提升性能，用户可以实现 emitUpdateWithRetract 方法。
     * 这个方法在 retract 模式下会增量的输出结果，比如有数据更新了，我们必须要撤回老的数据，然后再发送新的数据。
     * 如果定义了 emitUpdateWithRetract 方法，那它会优先于 emitValue 方法被使用，
     * 因为一般认为 emitUpdateWithRetract 会更加高效，因为它的输出是增量的
     */

    // 在批式聚合以及窗口聚合中是必须要实现的
    // case 1
    public void emitValue(Top2Accum acc, Collector<Tuple2<Integer, Integer>> out) {
        // emit the value and rank.
        if (acc.first != Integer.MIN_VALUE) {
            out.collect(Tuple2.of(acc.first, 1));
        }
        if (acc.second != Integer.MIN_VALUE) {
            out.collect(Tuple2.of(acc.second, 2));
        }
    }

    // 在 retract 模式下，该方法负责发送被更新的值
    // case 2
    // 下面的例子展示了如何使用 emitUpdateWithRetract 方法来只发送更新的数据。为了只发送更新的结果，accumulator 保存了上一次的最大的2个值，
    // 也保存了当前最大的2个值。注意：如果 TopN 中的 n 非常大，这种既保存上次的结果，也保存当前的结果的方式不太高效。一种解决这种问题的方式是把
    // 输入数据直接存储到 accumulator 中，然后在调用 emitUpdateWithRetract 方法时再进行计算。
    public void emitUpdateWithRetract(Top2Accum acc, RetractableCollector<Tuple2<Integer, Integer>> out) {
        if (!acc.first.equals(acc.oldFirst)) {
            if (acc.oldFirst != Integer.MIN_VALUE) {
                out.retract(Tuple2.of(acc.oldFirst, 1));
            }
            out.collect(Tuple2.of(acc.first, 1));
            acc.oldFirst = acc.first;
        }

        if (!acc.second.equals(acc.oldSecond)) {
            if (acc.oldSecond != Integer.MIN_VALUE) {
                out.retract(Tuple2.of(acc.oldSecond, 2));
            }
            out.collect(Tuple2.of(acc.second, 2));
            acc.oldSecond = acc.second;
        }
    }

}
