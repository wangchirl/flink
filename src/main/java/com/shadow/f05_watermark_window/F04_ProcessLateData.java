package com.shadow.f05_watermark_window;

import com.shadow.f00_pojo.ClickEvent;
import com.shadow.f00_pojo.UrlViewCount;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * 4、迟到数据的处理
 * 1）设置watermark延迟时间
 *   逻辑时间调慢
 * 2）允许窗口处理迟到数据，设置窗口延迟关闭
 *   默认情况下水位线到达窗口结束时间时会关闭窗口并进行计算输出结果
 *   设置窗口延迟关闭后，水位线到达时会输出一个计算结果值，后续在延迟关闭时间内每达到一个延迟数据会进行一次计算并输出结果，直到延迟时间结束
 * 3）侧输出流，将最后的迟到数据输出到侧输出流
 *    最终的兜底处理
 */
public class F04_ProcessLateData {
    public static void main(String[] args) throws Exception {
        // 1、获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2、添加数据源
        SingleOutputStreamOperator<ClickEvent> stream =
                env.socketTextStream("localhost", 7777)
                        .map(new MapFunction<String, ClickEvent>() {
                            @Override
                            public ClickEvent map(String value) throws Exception {
                                String[] fields = value.split(" ");
                                return new ClickEvent(fields[0].trim(), fields[1].trim(), Long.valueOf(fields[2].trim()));
                            }
                        })
                        // 方式一：设置watermark延迟时间，2秒钟
                        .assignTimestampsAndWatermarks(WatermarkStrategy.<ClickEvent>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner(new SerializableTimestampAssigner<ClickEvent>() {
                                    @Override
                                    public long extractTimestamp(ClickEvent element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                }));

        // 定义侧输出流标签
        OutputTag<ClickEvent> outputTag = new OutputTag<ClickEvent>("late") {
        };

        // 3、数据转换
        SingleOutputStreamOperator<UrlViewCount> result = stream.keyBy(data -> data.url)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                // 方式二：允许窗口处理迟到数据，设置1分钟的等待时间
                .allowedLateness(Time.minutes(1))
                // 方式三：将最后的迟到数据输出到侧输出流
                .sideOutputLateData(outputTag)
                .aggregate(new UrlViewCountAgg(), new UrlViewCountResult());

        // 4、数据输出
        result.print("result");
        result.getSideOutput(outputTag).print("late");
        stream.print("input");

        // 5、执行
        env.execute();
    }

    public static class UrlViewCountAgg implements AggregateFunction<ClickEvent, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(ClickEvent value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return null;
        }
    }

    public static class UrlViewCountResult extends ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow> {

        @Override
        public void process(String url, Context context, Iterable<Long> elements, Collector<UrlViewCount> out) throws Exception {
            // 结合窗口信息，包装输出内容
            Long start = context.window().getStart();
            Long end = context.window().getEnd();
            out.collect(new UrlViewCount(url, elements.iterator().next(), start, end));
        }
    }
}
