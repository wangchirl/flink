package com.shadow.f03_transform;

import com.shadow.f00_pojo.ClickEvent;
import com.shadow.f02_source.ClickParallelSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 6、User Definition Function (UDF) 用户自定义函数
 * 6.1 函数类 ：MapFunction、FilterFunction、FlatMapFunction、ReduceFunction...
 * 6.2 匿名函数（Lambda）
 * 6.3 富函数类：RichMapFunction、RichFilterFunction、RichFlatMapFunction、RichReduceFunction...
 */
public class T06_CustomUDF {

    public static void main(String[] args) throws Exception {
        // 1、获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2、添加数据源
        DataStreamSource<ClickEvent> clickDS = env.addSource(new ClickParallelSource());

        // 3、数据转换
        SingleOutputStreamOperator<String> result = clickDS.map(new StringMapFunction()) // 1、函数类
                .map(new MapFunction<String, String>() { // 2、匿名函数
                    @Override
                    public String map(String value) throws Exception {
                        return value + " anonymous |";
                    }
                }).map(data -> data + " lambda |") // 3、lambda
                .map(new RichStringMapFunction());// 4、富函数类

        // 4、数据输出
        result.print("UDF");

        // 5、执行
        env.execute();
    }

    // 1、函数类
    public static class StringMapFunction implements MapFunction<ClickEvent, String> {
        @Override
        public String map(ClickEvent value) throws Exception {
            return value.user + " function class | ";
        }
    }

    // 4、富函数类
    public static class RichStringMapFunction extends RichMapFunction<String, String> {
        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("open");
        }

        @Override
        public String map(String value) throws Exception {
            return value + " rich function class";
        }

        @Override
        public void close() throws Exception {
            System.out.println("close");
        }
    }

}
