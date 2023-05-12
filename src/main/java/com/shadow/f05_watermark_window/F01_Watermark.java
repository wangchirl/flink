package com.shadow.f05_watermark_window;

import com.shadow.f00_pojo.ClickEvent;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Calendar;
import java.util.Random;

/**
 * 时间分类：
 * 1）处理时间
 * 2）事件时间（重点）
 * 3）摄入时间
 * <p>
 * 1、水位线：watermark -> 描述数据流中时间进展的逻辑时钟
 * 表示：当前水位线时间 t 之前的数据都到齐了
 * 通过 assignTimestampsAndWatermarks() 指定水位线及事件时间
 * 默认水位线实现：
 * WatermarkStrategy.forMonotonousTimestamps() - 有序
 * WatermarkStrategy.forBoundedOutOfOrderness(Duration) - 无序
 * WatermarkStrategy.noWatermarks() - 不生成水位线
 * 注意关注：事件时间 eventTime
 * <p>
 * 水位线生成方式：
 * 1）按每条数据生成 - （太频繁）
 * 2）周期生成 - （默认200ms生成一条水位线）
 * 通过 WatermarkStrategy.withTimestampAssigner() 指定事件时间生成水位线
 * <p>
 * 水位线延迟：
 * 针对无序流，先产生的数据可能后被处理，水位线根据数据时间生成，水位线时间不能倒流
 * 可针对无序流设置水位线延迟
 * <p>
 * 自定义水位线：
 * 1）实现 WatermarkStrategy 接口，重写 createTimestampAssigner 方法
 * 2）创建 WatermarkGenerator 参考默认实现 BoundedOutOfOrdernessWatermarks
 * <p>
 * 设置水位线方式：
 * 1）Source - SourceContext#collectWithTimestamp() 、SourceContext#emitWatermark()
 * 2）DataStream - assignTimestampsAndWatermarks() - 更灵活
 * <p>
 * 水位线的默认计算公式：
 * 水位线 = 观察到的最大事件时间 – 最大延迟时间 – 1 毫秒
 */
public class F01_Watermark {

    public static void main(String[] args) throws Exception {
        // 1、获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2、添加数据源
        env.socketTextStream("localhost", 7777)
                .map(new MapFunction<String, ClickEvent>() {
                    @Override
                    public ClickEvent map(String value) throws Exception {
                        String[] fields = value.split(",");
                        return new ClickEvent(fields[0].trim(), fields[1].trim(), Long.valueOf(fields[2].trim()));
                    }
                })
                // 插入水位线的逻辑
                .assignTimestampsAndWatermarks(
                        // 针对乱序流插入水位线，延迟时间设置为5s
                        WatermarkStrategy.<ClickEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner(new SerializableTimestampAssigner<ClickEvent>() {
                                    // 抽取时间戳的逻辑
                                    @Override
                                    public long extractTimestamp(ClickEvent element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                })
                )
                // 根据user分组，开窗统计
                .keyBy(data -> data.user)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<ClickEvent, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<ClickEvent> elements, Collector<String> out) throws Exception {
                        Long start = context.window().getStart();
                        Long end = context.window().getEnd();
                        Long currentWatermark = context.currentWatermark();
                        Long count = elements.spliterator().getExactSizeIfKnown();
                        out.collect("窗口" + start + " ~ " + end + "中共有" + count + "个元素，窗口闭合计算时，水位线处于：" + currentWatermark);
                    }
                })
                .print();
        // 3、数据转换
        // 4、数据输出
        // 5、执行
        env.execute();
    }

    /**
     * 自定义水位线
     */
    public static class CustomWatermarkStrategy implements WatermarkStrategy<ClickEvent> {
        @Override
        public TimestampAssigner<ClickEvent> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            return new SerializableTimestampAssigner<ClickEvent>() {
                @Override
                public long extractTimestamp(ClickEvent element, long recordTimestamp) {
                    return element.timestamp; // 告诉程序数据源里的时间戳是哪一个字段
                }
            };
        }

        @Override
        public WatermarkGenerator<ClickEvent> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return new CustomPeriodicGenerator();
        }
    }

    public static class CustomPeriodicGenerator implements WatermarkGenerator<ClickEvent> {
        private Long delayTime = 5000L; // 延迟时间
        private Long maxTs = Long.MIN_VALUE + delayTime + 1L; // 观察到的最大时间戳

        @Override
        public void onEvent(ClickEvent event, long eventTimestamp, WatermarkOutput output) {
            // 每来一条数据就调用一次
            maxTs = Math.max(event.timestamp, maxTs); // 更新最大时间戳
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            // 发射水位线，默认200ms调用一次
            output.emitWatermark(new Watermark(maxTs - delayTime - 1L));
        }
    }

    /**
     * Source 生成水位线
     */
    public static class ClickSourceWithWatermark implements SourceFunction<ClickEvent> {
        private boolean running = true;

        @Override
        public void run(SourceContext<ClickEvent> sourceContext) throws Exception {
            Random random = new Random();
            String[] userArr = {"Mary", "Bob", "Alice"};
            String[] urlArr = {"./home", "./cart", "./prod?id=1"};
            while (running) {
                long currTs = Calendar.getInstance().getTimeInMillis(); // 毫秒时间戳
                String username = userArr[random.nextInt(userArr.length)];
                String url = urlArr[random.nextInt(urlArr.length)];
                ClickEvent event = new ClickEvent(username, url, currTs);
                // 使用collectWithTimestamp方法将数据发送出去，并指明数据中的时间戳的字段
                sourceContext.collectWithTimestamp(event, event.timestamp);
                // 发送水位线
                sourceContext.emitWatermark(new org.apache.flink.streaming.api.watermark.Watermark(event.timestamp - 1L));
                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
