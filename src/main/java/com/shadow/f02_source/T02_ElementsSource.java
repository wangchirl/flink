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
 * 2、直接构建数据
 * fromElements()
 */
public class T02_ElementsSource {

    public static void main(String[] args) throws Exception {
        // 1、获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2、添加数据源
        DataStreamSource<ClickEvent> eventsDS = env.fromElements(
                new ClickEvent("shadow", "./home", 1000L),
                new ClickEvent("oracle", "./index", 2000L),
                new ClickEvent("shadow", "./cart", 3000L)
        );

        // 3、数据转换
        SingleOutputStreamOperator<Tuple2<String, Long>> aggResult = eventsDS.flatMap(new FlatMapFunction<ClickEvent, Tuple2<String, Long>>() {
            @Override
            public void flatMap(ClickEvent value, Collector<Tuple2<String, Long>> out) throws Exception {
                if (value.user.equals("shadow")) {
                    out.collect(Tuple2.of(value.user, 1L));
                }
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
