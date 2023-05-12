package com.shadow.f02_source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.pulsar.source.PulsarSource;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;
import org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.pulsar.client.api.SubscriptionType;

/**
 * 6、Pulsar 读取数据
 * https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/connectors/datastream/pulsar/
 * <p>
 * 6.1 启动 zk
 * 6.2 启动 bookie
 * 6.3 启动 bookkeeper
 */
public class T06_PulsarSource {

    public static void main(String[] args) throws Exception {
        // 1、获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2、添加数据源
        PulsarSource<String> source = PulsarSource.builder()
                .setServiceUrl("") // TODO
                .setAdminUrl("") // TODO
                .setStartCursor(StartCursor.earliest())
                .setTopics("flink-topic")
                .setDeserializationSchema(PulsarDeserializationSchema.flinkSchema(new SimpleStringSchema()))
                .setSubscriptionName("flink-subscription")
                .setSubscriptionType(SubscriptionType.Exclusive)
                .build();

        DataStreamSource<String> pulsarDS = env.fromSource(source, WatermarkStrategy.forMonotonousTimestamps(), "Pulsar Source");

        // 3、数据转换
        SingleOutputStreamOperator<Tuple2<String, Long>> aggResult = pulsarDS.flatMap((FlatMapFunction<String, String>) (value, out) -> {
            String[] words = value.split(" ");
            for (String word : words) {
                out.collect(word);
            }
        }).returns(Types.STRING)
                .map((MapFunction<String, Tuple2<String, Long>>) value -> Tuple2.of(value, 1L)).returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(data -> data.f0)
                .sum(1);

        // 4、数据输出
        aggResult.print("sum");

        // 5、执行
        env.execute();
    }
}
