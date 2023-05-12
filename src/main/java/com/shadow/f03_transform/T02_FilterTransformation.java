package com.shadow.f03_transform;

import com.shadow.f00_pojo.ClickEvent;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 2、filter() 过滤 -> 不会改变输入的类型
 *
 * @see org.apache.flink.api.common.functions.FilterFunction
 * - filter 过滤方法
 * @see org.apache.flink.api.common.functions.RichFilterFunction
 * - filter 过滤方法
 * - open 生命周期开始方法
 * - close 生命周期结束方法
 * - getRuntimeContext 获取运行上下文方法
 */
public class T02_FilterTransformation {

    public static void main(String[] args) throws Exception {
        // 1、获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2、添加数据源
        DataStreamSource<ClickEvent> clickDS = env.fromElements(
                new ClickEvent("shadow", "./index", 2000L),
                new ClickEvent("oracle", "./home", 1000L),
                new ClickEvent("oracle", "./cart", 1000L)
        );

        // 3、数据转换
        // SingleOutputStreamOperator<ClickEvent> filterResult = clickDS.filter(new OracleFilterFunction());
        SingleOutputStreamOperator<ClickEvent> filterResult = clickDS.filter(new OracleRichFilterFunction());
        // lambda 表达式方式
        // SingleOutputStreamOperator<ClickEvent> filterResult = clickDS.filter(item -> item.user.equals("oracle"));

        // 4、数据输出
        filterResult.print("filter");

        // 5、执行
        env.execute();
    }

    // 1、定义 FilterFunction
    public static class OracleFilterFunction implements FilterFunction<ClickEvent> {
        @Override
        public boolean filter(ClickEvent value) throws Exception {
            return value.user.equals("oracle");
        }
    }

    // 2、定义功能更强大的 RichFilterFunction
    public static class OracleRichFilterFunction extends RichFilterFunction<ClickEvent> {

        @Override
        public void open(Configuration parameters) throws Exception {
            JobID jobId = getRuntimeContext().getJobId();
            System.out.println("jobId = " + jobId);
        }

        @Override
        public boolean filter(ClickEvent value) throws Exception {
            return value.user.equals("oracle");
        }

        @Override
        public void close() throws Exception {
            int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
            System.out.println("indexOfThisSubtask = " + indexOfThisSubtask);
        }
    }
}
