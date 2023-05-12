package com.shadow.f01_env;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 2、流处理执行环境
 * 2.1）StreamExecutionEnvironment
 * 1. getExecutionEnvironment() -> 自行决定该返回什么样的运行环境
 * 2. createLocalEnvironment() -> 返回一个本地执行环境
 * 3. createRemoteEnvironment(ip,port,jarpath) -> 返回集群执行环境
 * <p>
 * 2.2）由于流处理API进行了批流统一，可以指定执行模式：
 * 1. setRuntimeMode(RuntimeExecutionMode)
 * RuntimeExecutionMode:
 * STREAMING -> 默认流处理
 * BATCH -> 批处理
 * AUTOMATIC -> 自动判断
 * 2. 命令行指定: bin/flink run -Dexecution.runtime-mode=BATCH
 * <p>
 * 2.3）其他全局设置
 * 1.setParallelism() 设置并行读
 * 2.disableOperatorChaining() 全局禁用算子链
 * 3.setStateBackend() 设置状态后端
 * 4.setRestartStrategy() 设置重启策略
 */
public class T02_StreamEnvBounded {

    public static void main(String[] args) throws Exception {
        // 1、获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        // StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", 8082, "/xxx/FlinkTutorial-1.0-SNAPSHOT.jar");
        // env.setParallelism(1);
        // env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        // env.disableOperatorChaining();
        // env.setStateBackend(new HashMapStateBackend());
        // env.setRestartStrategy(RestartStrategies.noRestart());

        // 2、设置数据源
        DataStreamSource<String> lineDS = env.readTextFile("input/words.txt");

        // 3、数据转换
        SingleOutputStreamOperator<Tuple2<String, Long>> aggResult = lineDS.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(word);
                }
            }
        }).returns(Types.STRING)
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String value) throws Exception {
                        return Tuple2.of(value, 1L);
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
