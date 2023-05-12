package com.shadow.f04_sink;

import com.shadow.f00_pojo.ClickEvent;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * 2、addSink 输出到 Kafka
 * 2.1 FlinkKafkaProducer - 老版本
 * - addSink(FlinkKafkaProducer)
 * 2.2 KafkaSink - 新版本
 * - sinkTo(KafkaSink)
 * <p>
 * 启动 ZK
 * 下载 ZK -> https://www.apache.org/dyn/closer.lua/zookeeper/zookeeper-3.7.1/apache-zookeeper-3.7.1-bin.tar.gz
 * ./bin/zkServer.sh
 * 启动 Kafka
 * ./bin/kafka-server-start.sh start config/properties
 * 启动消费者
 * ./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic clicks
 */
public class T02_KafkaSink {

    public static void main(String[] args) throws Exception {
        // 1、获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2、添加数据源
        DataStreamSource<ClickEvent> clickDS = env.fromElements(
                new ClickEvent("shadow", "./index", 1000L),
                new ClickEvent("oracle", "./index", 2000L),
                new ClickEvent("shadow", "./cart", 3000L),
                new ClickEvent("oracle", "./home", 5000L)
        );

        // 3、数据转换
        SingleOutputStreamOperator<String> result = clickDS.map(ClickEvent::toString);

        // 4、数据输出
        // 4.1 老版本
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        FlinkKafkaProducer<String> kafkaSink = new FlinkKafkaProducer<>("clicks",
                new SimpleStringSchema(),
                properties);
//        result.addSink(kafkaSink);

        Thread.sleep(2000);

        // 4.2 新版本
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("clicks")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
        result.sinkTo(sink);

        // 5、执行
        env.execute();
    }
}
