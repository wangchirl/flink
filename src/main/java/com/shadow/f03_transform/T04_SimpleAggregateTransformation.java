package com.shadow.f03_transform;

import com.shadow.f00_pojo.ClickEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 4、简单聚合 keyBy 之后基于 KeyedStream
 * - sum()：在输入流上，对指定的字段做叠加求和的操作
 * - min()：在输入流上，对指定的字段求最小值【其他值为第一条记录的】，如果想得到最小记录的整个数据使用 minBy
 * - max()：在输入流上，对指定的字段求最大值【其他值为第一条记录的】，如果想得到最大记录的整个数据使用 maxBy
 * - minBy()：与 min()类似，在输入流上针对指定字段求最小值。不同的是，min()只计
 * 算指定字段的最小值，其他字段会保留最初第一个数据的值；而 minBy()则会返回包含字段最小值的整条数据
 * - maxBy()：与 max()类似，在输入流上针对指定字段求最大值。两者区别与min()/minBy()完全一致
 */
public class T04_SimpleAggregateTransformation {

    public static void main(String[] args) throws Exception {
        // 1、获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2、添加数据源
        DataStreamSource<ClickEvent> clickDS = env.fromElements(
                new ClickEvent("oracle", "./home", 12000L),
                new ClickEvent("shadow", "./index", 1000L),
                new ClickEvent("shadow", "./prod", 11000L),
                new ClickEvent("oracle", "./cart", 3000L)
        );

        // 3、数据转换
        // 3.1 sum
        SingleOutputStreamOperator<Tuple2<String, Long>> sumResult = clickDS
                .map(new MapFunction<ClickEvent, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(ClickEvent value) throws Exception {
                        return Tuple2.of(value.user, 1L);
                    }
                }).returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(item -> item.f0) // 分组
                .sum(1); // 元组下标位置

        // 3.2 min
        SingleOutputStreamOperator<Tuple3<String, Long, ClickEvent>> minResult = clickDS
                .map(new MapFunction<ClickEvent, Tuple3<String, Long, ClickEvent>>() {
                    @Override
                    public Tuple3<String, Long, ClickEvent> map(ClickEvent value) throws Exception {
                        return Tuple3.of(value.user, value.timestamp, value);
                    }
                }).returns(Types.TUPLE(Types.STRING, Types.LONG, Types.POJO(ClickEvent.class)))
                .keyBy(item -> item.f0) // 分组
                .min(1); // 元组下标位置

        // 3.3 minBy
        SingleOutputStreamOperator<Tuple3<String, Long, ClickEvent>> minByResult = clickDS
                .map(new MapFunction<ClickEvent, Tuple3<String, Long, ClickEvent>>() {
                    @Override
                    public Tuple3<String, Long, ClickEvent> map(ClickEvent value) throws Exception {
                        return Tuple3.of(value.user, value.timestamp, value);
                    }
                }).returns(Types.TUPLE(Types.STRING, Types.LONG, Types.POJO(ClickEvent.class)))
                .keyBy(item -> item.f0) // 分组
                .minBy(1); // 元组下标位置

        // 3.4 max
        SingleOutputStreamOperator<ClickEvent> maxResult = clickDS
                .keyBy(item -> item.user) // 分组
                .max("timestamp");  // pojo 字段名称

        // 3.5 maxBy
        SingleOutputStreamOperator<ClickEvent> maxByResult = clickDS
                .keyBy(item -> item.user) // 分组
                .maxBy("timestamp"); // pojo 字段名称

        // 4、数据输出
        sumResult.print("sum");
        minResult.print("min");
        minByResult.print("minBy");
        maxResult.print("max");
        maxByResult.print("maxBy");

        // 5、执行
        env.execute();
    }
}
