package com.shadow.f01_env;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 3、流处理
 * 1）Windows：
 * https://eternallybored.org/misc/netcat/
 * cmd -> nc.exe -> nc -lp 7777
 * 2）Linux：
 * nc -lk 7777
 */
public class T03_StreamEnvFlow {

    public static void main(String[] args) throws Exception {
        // 1、获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2、添加数据源
        DataStreamSource<String> lineDS = env.socketTextStream("localhost", 7777);

        // 3、数据转换
        SingleOutputStreamOperator<Tuple2<String, Long>> aggResult = lineDS.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1L));
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
