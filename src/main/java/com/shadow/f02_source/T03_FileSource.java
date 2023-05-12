package com.shadow.f02_source;

import com.shadow.f00_pojo.ClickEvent;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 3、从文件读取数据：
 * env.readTextFile()
 * 1. 参数可以是目录，也可以是文件
 * 2. 路径可以是相对路径，也可以是绝对路径
 * 3. 相对路径是从系统属性 user.dir 获取路径: idea 下是 project 的根目录, standalone 模式下是集群节点根目录
 * 4. 也可以从 hdfs 目录下读取, 使用路径 hdfs://..., 由于 Flink 没有提供 hadoop 相关依赖,需要 pom 中添加相关依赖
 * <dependency>
 *  <groupId>org.apache.hadoop</groupId>
 *  <artifactId>hadoop-client</artifactId>
 *  <version>2.7.5</version>
 *  <scope>provided</scope>
 * </dependency>
 */
public class T03_FileSource {

    public static void main(String[] args) throws Exception {
        // 1、获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 2、添加数据源
        DataStreamSource<String> fileDS = env.readTextFile("input/clicks.csv");

        // 3、数据转换
        SingleOutputStreamOperator<Tuple2<String, Long>> aggResult = fileDS.flatMap(new FlatMapFunction<String, ClickEvent>() {
            @Override
            public void flatMap(String value, Collector<ClickEvent> out) throws Exception {
                String[] data = value.split(",");
                out.collect(new ClickEvent(data[0].trim(), data[1].trim(), Long.valueOf(data[2].trim())));
            }
        }).returns(Types.POJO(ClickEvent.class))
                .map(new MapFunction<ClickEvent, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(ClickEvent value) throws Exception {
                        return Tuple2.of(value.user, 1L);
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
