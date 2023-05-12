package com.shadow.f03_transform;

import com.shadow.f00_pojo.ClickEvent;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 3、flatMap() 扁平映射 -> 可以改变输入的类型
 * 可同时满足 map 和 filter 的功能
 *
 * @see org.apache.flink.api.common.functions.FlatMapFunction
 * - flatMap 处理方法
 * @see org.apache.flink.api.common.functions.RichFlatMapFunction
 * - flatMap 处理方法
 * - open 生命周期开始方法
 * - close 生命周期结束方法
 * - getRuntimeContext 获取运行上下文方法
 */
public class T03_FlatMapTransformation {

    public static void main(String[] args) throws Exception {
        // 1、获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2、添加数据源
        DataStreamSource<ClickEvent> clickDS = env.fromElements(
                new ClickEvent("oracle", "./home", 2000L),
                new ClickEvent("shadow", "./index", 1000L),
                new ClickEvent("oracle", "./cart", 3000L)
        );

        // 3、数据转换
        // SingleOutputStreamOperator<Tuple2<String, String>> flatMapResult = clickDS.flatMap(new OracleFlatMapFunction()).returns(Types.TUPLE(Types.STRING, Types.STRING));
        SingleOutputStreamOperator<Tuple2<String, String>> flatMapResult = clickDS.flatMap(new OracleRichFlatMapFunction())
                .returns(Types.TUPLE(Types.STRING, Types.STRING));
        // lambda 表达式方式
        /*SingleOutputStreamOperator<Tuple2<String, String>> flatMapResult = clickDS.flatMap((ClickEvent item, Collector<Tuple2<String, String>> out) -> {
            if (item.user.equals("oracle")) {
                out.collect(Tuple2.of(item.user, item.url));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.STRING));*/

        // 4、数据输出
        flatMapResult.print("flatMap");

        // 5、执行
        env.execute();
    }

    // 1、定义 FlatMapFunction
    public static class OracleFlatMapFunction implements FlatMapFunction<ClickEvent, Tuple2<String, String>> {
        @Override
        public void flatMap(ClickEvent value, Collector<Tuple2<String, String>> out) throws Exception {
            if (value.user.equals("oracle")) {
                out.collect(Tuple2.of(value.user, value.url));
            }
        }
    }

    // 2、定义功能更强大的 RichFlatMapFunction
    public static class OracleRichFlatMapFunction extends RichFlatMapFunction<ClickEvent, Tuple2<String, String>> {
        @Override
        public void open(Configuration parameters) throws Exception {
            JobID jobId = getRuntimeContext().getJobId();
            System.out.println("jobId = " + jobId);
        }

        @Override
        public void flatMap(ClickEvent value, Collector<Tuple2<String, String>> out) throws Exception {
            if (value.user.equals("oracle")) {
                out.collect(Tuple2.of(value.user, value.url));
            }
        }

        @Override
        public void close() throws Exception {
            int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
            System.out.println("indexOfThisSubtask = " + indexOfThisSubtask);
        }
    }
}
