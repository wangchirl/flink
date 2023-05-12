package com.shadow.f02_source;

import com.shadow.f00_pojo.ClickEvent;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 7、自定义数据源
 * 7.1 实现接口 SourceFunction
 * run()方法：使用运行时上下文对象（SourceContext）向下游发送数据
 * cancel()方法：通过标识位控制退出循环，来达到中断数据源的效果
 * note：其 SourceFunction 的并行度只能是 1，否则报错
 * <p>
 * 7.2 实现接口 ParallelSourceFunction
 * 支持并行度设置
 */
public class T071_CustomSource {

    public static void main(String[] args) throws Exception {
        // 1、获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // note 并行度只能是1
        DataStreamSource<ClickEvent> clickDS = env.addSource(new ClickSource())/*.setParallelism(2)*/;

        // 3、数据转换
        SingleOutputStreamOperator<Tuple2<String, Long>> aggResult = clickDS.flatMap(new FlatMapFunction<ClickEvent, Tuple2<String, Long>>() {
            @Override
            public void flatMap(ClickEvent value, Collector<Tuple2<String, Long>> out) throws Exception {
                out.collect(Tuple2.of(value.user, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(data -> data.f0)
                .sum(1);

        // 4、数据输出
        aggResult.print("sum");

        // 5、执行
        env.execute();

    }

}


