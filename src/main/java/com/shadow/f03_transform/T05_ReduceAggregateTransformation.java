package com.shadow.f03_transform;

import com.shadow.f00_pojo.ClickEvent;
import com.shadow.f02_source.ClickParallelSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 5、reduce 规约聚合 -> 不会改变输入的类型
 * 是一个一般化的聚合统计操作
 *
 * @see org.apache.flink.api.common.functions.ReduceFunction
 * 内部会维护一个初始值为空的累加器，累加器的类型和输入元素的类型相同
 * 当第一条元素到来时，累加器的值更新为第一条元素的值，当新的元素到来时
 * 新元素会和累加器进行累加操作，这里的累加操作就是 reduce 函数定义的运算规则
 * 然后将更新以后的累加器的值向下游输出
 * @see org.apache.flink.api.common.functions.RichReduceFunction
 * - reduce 规约方法
 * - open 生命周期开始方法
 * - close 生命周期结束方法
 * - getRuntimeContext 获取运行上下文方法
 * <p>
 * 因为状态不会清空，所以我们需要将 reduce 算子作用在一个有限 key 的流上
 */
public class T05_ReduceAggregateTransformation {

    public static void main(String[] args) throws Exception {
        // 1、获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2、添加数据源
        DataStreamSource<ClickEvent> clickDS = env.addSource(new ClickParallelSource());

        // 3、数据转换
        SingleOutputStreamOperator<Tuple2<String, Long>> reduceResult = clickDS.map(new MapFunction<ClickEvent, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(ClickEvent value) throws Exception {
                return Tuple2.of(value.user, 1L);
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(data -> data.f0) // 按 user 分组规约
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        // 统计每个 user 的点击次数
                        return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                    }
                }).keyBy(data -> "all") // 全部进入一个组
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        // 找到组内点击数最大的
                        return value1.f1 > value2.f1 ? value1 : value2;
                    }
                });

        // 4、数据输出
        reduceResult.print("reduce");

        // 5、执行
        env.execute();
    }
}
