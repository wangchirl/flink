package com.shadow.f02_source;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 4、Socket 读取数据
 * env.socketTextStream()
 * 启动 socket 服务：
 * WINDOWS:
 * 1.下载 netcat 服务 https://eternallybored.org/misc/netcat/
 * 2.cmd -> nc.exe -lp 7777
 * LINUX:
 * nc -lk 7777
 */
public class T04_SocketSource {

    public static void main(String[] args) throws Exception {
        // 1、获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2、添加数据源
        DataStreamSource<String> socketDS = env.socketTextStream("localhost", 7777);

        // 3、数据转换
        SingleOutputStreamOperator<Tuple2<String, Long>> aggResult = socketDS.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(word);
                }
            }
        }).returns(Types.STRING)
                .map(data -> Tuple2.of(data, 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(data -> data.f0)
                .sum(1);

        // 4、数据输出
        aggResult.print("sum");

        // 5、执行
        env.execute();
    }
}
