package com.shadow.f04_sink;

import com.shadow.f00_pojo.ClickEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;

/**
 * 1、addSink -> 输出到文件
 * 1.1 writeAsText() - 过期方法，直接写出到文件
 * 1.2 writeAsCsv() - 过期方法，直接写出到 csv 文件，要求类型必须是 Tuple 类型
 * 1.3 StreamingFileSink -> 推荐方法
 * - forRowFormat - 行编码
 * - forBulkFormat - 批量编码
 */
public class T01_FileSink {

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
        // 4.1 writeAsText
        result.writeAsText("output/text.txt", FileSystem.WriteMode.OVERWRITE);

        // 3、数据转换
        SingleOutputStreamOperator<Tuple2<String, String>> csv = clickDS.map(new MapFunction<ClickEvent, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(ClickEvent value) throws Exception {
                return Tuple2.of(value.user, value.url);
            }
        });
        // 4.2 writeAsCsv -> 数据格式必须是 Tuple 类型
        csv.writeAsCsv("output/click.csv", FileSystem.WriteMode.OVERWRITE);

        // 4.3 StreamingFileSink
        StreamingFileSink<String> streamingFileSink = StreamingFileSink.forRowFormat(new Path("./output"),
                new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(DefaultRollingPolicy.builder()
                        .withRolloverInterval(TimeUnit.MINUTES.toMillis(15)) // 至少包含 15 分钟的数据
                        .withInactivityInterval(TimeUnit.MINUTES.toMillis(5)) // 最近 5 分钟没有收到新的数据
                        .withMaxPartSize(1024 * 1024 * 1024) // 文件大小已达到 1 GB
                        .build())
                .build();
        clickDS.map(ClickEvent::toString).addSink(streamingFileSink);

        // 5、执行
        env.execute();
    }
}
