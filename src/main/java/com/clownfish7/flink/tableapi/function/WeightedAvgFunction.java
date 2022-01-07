package com.clownfish7.flink.tableapi.function;

import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.types.inference.TypeInference;

/**
 * classname WeightedAvgFunction
 * description 聚合函数
 * <p>
 * createAccumulator()
 * accumulate()
 * getValue()
 * 是每个 AggregateFunction 必须实现的
 * <p>
 * <p>
 * retract() 在 bounded OVER 窗口中是必须实现的
 * merge() 在许多批式聚合和会话以及滚动窗口聚合中是必须实现的。除此之外，这个方法对于优化也很多帮助。例如，两阶段聚合优化就需要所有的 AggregateFunction 都实现 merge 方法
 * resetAccumulator() 在许多批式聚合中是必须实现的
 * create 2022-01-07 9:59
 */
public class WeightedAvgFunction extends AggregateFunction<Integer, WeightedAvgAccum> {


    @Override // 这个方法会在一次聚合操作的开始调用一次，主要用于构造一个Accumulator，用于存储在聚合过程中的临时对象
    public WeightedAvgAccum createAccumulator() {
        return new WeightedAvgAccum();
    }

    @Override // 这个方法是在聚合结束以后，对中间结果做处理，然后将结果返回，最终sql中得到的结果数据就是这个值
    public Integer getValue(WeightedAvgAccum accumulator) {
        if (accumulator.getCount() == 0) {
            return null;
        } else {
            return accumulator.getSum() / accumulator.getCount();
        }
    }

    // 这个方法，每来一条数据会调用一次这个方法，我们就在这个方法里实现我们的聚合函数的具体逻辑
    public void accumulate(WeightedAvgAccum acc, int inputValue, int inputWeight) {
        acc.setSum(acc.getSum() + inputWeight * inputValue);
        acc.setCount(acc.getCount() + inputWeight);
    }

    // 表示具体的回撤操作，对于自定义聚合函数，如果其接受到的是撤回流那么就必须实现该方法
    // retract() 在 bounded OVER 窗口中的聚合函数必须要实现
    public void retract(WeightedAvgAccum acc, int inputValue, int inputWeight) {
        acc.setSum(acc.getSum() - inputWeight * inputValue);
        acc.setCount(acc.getCount() - inputWeight);
    }

    // merge() 在许多批式聚合和以及流式会话和滑动窗口聚合中是必须要实现的
    public void merge(WeightedAvgAccum acc, Iterable<WeightedAvgAccum> it) {
        for (WeightedAvgAccum a : it) {
            acc.setSum(acc.getSum() + a.getSum());
            acc.setCount(acc.getCount() + a.getCount());
        }
    }

    // resetAccumulator() 在许多批式聚合中是必须要实现的
    public void resetAccumulator(WeightedAvgAccum accum) {
        accum.setSum(0);
        accum.setCount(0);
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return super.getTypeInference(typeFactory);
    }
}

