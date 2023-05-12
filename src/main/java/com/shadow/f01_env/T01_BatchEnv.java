package com.shadow.f01_env;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 1、批处理 API 执行环境
 * ExecutionEnvironment
 * 由于Flink对批流进行了统一，后续直接使用流处理执行环境
 */
public class T01_BatchEnv {
    public static void main(String[] args) throws Exception {
        // 1、获取执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2、设置数据源
        DataSource<String> lineDS = env.readTextFile("input/words.txt");

        // 3、数据转换
        AggregateOperator<Tuple2<String, Long>> aggResult = lineDS.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1L));
                }
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG))
                .groupBy(0)
                .sum(1);

        // 4、数据输出
        aggResult.print();

    }
}
