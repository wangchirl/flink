package com.shadow.f07_multiple_stream;

import com.shadow.f00_pojo.ClickEvent;
import com.shadow.f02_source.ClickSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 1、分流
 * - filter
 * 通过 filter() 筛选出符合添加的数据
 * 问题：重复使用数据源，数据流的复制，不够高效
 * <p>
 * - outputTag 侧输出流
 * 通过侧输出流进行分流处理
 * OutputTag 及 ProcessFunction 处理函数
 */
public class F01_SplitFlow {

    // 定义输出标签，侧输出流的数据类型为三元组(user, url, timestamp)
    private static OutputTag<Tuple3<String, String, Long>> ShadowTag = new OutputTag<Tuple3<String, String, Long>>("Shadow-pv") {
    };
    private static OutputTag<Tuple3<String, String, Long>> OracleTag = new OutputTag<Tuple3<String, String, Long>>("Oracle-pv") {
    };

    public static void main(String[] args) throws Exception {
        // 1、filter
        filter();

        // 2、outputTag
        outputTag();
    }

    private static void filter() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<ClickEvent> stream = env
                .addSource(new ClickSource());
        // 筛选shadow的浏览行为放入ShadowStream流中
        DataStream<ClickEvent> ShadowStream = stream.filter(new FilterFunction<ClickEvent>() {
            @Override
            public boolean filter(ClickEvent value) throws Exception {
                return value.user.equals("shadow");
            }
        });
        // 筛选oracle的购买行为放入OracleStream流中
        DataStream<ClickEvent> OracleStream = stream.filter(new FilterFunction<ClickEvent>() {
            @Override
            public boolean filter(ClickEvent value) throws Exception {
                return value.user.equals("oracle");
            }
        });
        // 筛选其他人的浏览行为放入elseStream流中
        DataStream<ClickEvent> elseStream = stream.filter(new FilterFunction<ClickEvent>() {
            @Override
            public boolean filter(ClickEvent value) throws Exception {
                return !value.user.equals("shadow") && !value.user.equals("oracle");
            }
        });

        ShadowStream.print("shadow pv");
        OracleStream.print("oracle pv");
        elseStream.print("else pv");

        env.execute();
    }

    private static void outputTag() throws Exception {
        // 1、获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 2、添加数据源
        SingleOutputStreamOperator<ClickEvent> stream = env
                .addSource(new ClickSource());
        // 3、数据转换
        SingleOutputStreamOperator<ClickEvent> processedStream = stream.process(new ProcessFunction<ClickEvent, ClickEvent>() {
            @Override
            public void processElement(ClickEvent value, Context ctx, Collector<ClickEvent> out) throws Exception {
                if (value.user.equals("shadow")) {
                    ctx.output(ShadowTag, new Tuple3<>(value.user, value.url, value.timestamp));
                } else if (value.user.equals("oracle")) {
                    ctx.output(OracleTag, new Tuple3<>(value.user, value.url, value.timestamp));
                } else {
                    out.collect(value);
                }
            }
        });
        // 4、输出
        processedStream.getSideOutput(ShadowTag).print("Shadow pv");
        processedStream.getSideOutput(OracleTag).print("Oracle pv");
        processedStream.print("else");
        // 5、执行
        env.execute();
    }
}
