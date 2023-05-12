package com.shadow.f03_transform;

import com.shadow.f00_pojo.ClickEvent;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 1、map() 映射 -> 可以改变输入的类型
 *
 * @see org.apache.flink.api.common.functions.MapFunction
 * - map 转换方法
 * @see org.apache.flink.api.common.functions.RichMapFunction
 * - map 转换方法
 * - open 生命周期开始方法
 * - close 生命周期结束方法
 * - getRuntimeContext 获取运行上下文方法
 */
public class T01_MapTransformation {

    public static void main(String[] args) throws Exception {
        // 1、获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2、添加数据源
        DataStreamSource<ClickEvent> clickDS = env.fromElements(
                new ClickEvent("oracle", "./home", 1000L),
                new ClickEvent("shadow", "./index", 1000L)
        );

        // 3、数据转换
        // SingleOutputStreamOperator<String> mapResult = clickDS.map(new StringMapFunction());
        SingleOutputStreamOperator<String> mapResult = clickDS.map(new StringRichMapFunction());
        // lambda 表达式方式
        // SingleOutputStreamOperator<String> mapResult = clickDS.map(item -> item.user);

        // 4、数据输出
        mapResult.print("map");

        // 5、执行
        env.execute();
    }

    // 1、定义 MapFunction
    public static class StringMapFunction implements MapFunction<ClickEvent, String> {
        @Override
        public String map(ClickEvent value) throws Exception {
            return value.user;
        }
    }

    // 2、定义功能更强大的 RichMapFunction
    public static class StringRichMapFunction extends RichMapFunction<ClickEvent, String> {
        // 生命周期方法
        @Override
        public void open(Configuration parameters) throws Exception {
            JobID jobId = getRuntimeContext().getJobId();
            System.out.println("jobId = " + jobId);
        }

        @Override
        public String map(ClickEvent value) throws Exception {
            return value.user;
        }

        @Override
        public void close() throws Exception {
            int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
            System.out.println("indexOfThisSubtask = " + indexOfThisSubtask);
        }
    }
}
