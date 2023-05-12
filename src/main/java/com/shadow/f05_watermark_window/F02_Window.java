package com.shadow.f05_watermark_window;

import com.shadow.f00_pojo.ClickEvent;
import com.shadow.f00_pojo.UrlViewCount;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Objects;

/**
 * 2、窗口 Window
 * 窗口分类：
 * 1）时间窗口（滚动、滑动、会话，全局） - 分处理时间窗口和事件时间窗口
 * TumblingEventTimeWindows/TumblingProcessingTimeWindows、SlidingEventTimeWindows/SlidingProcessingTimeWindows
 * 2）计数窗口（滚动、滑动，全局） - 底层都是基于全局窗口实现
 * <p>
 * 窗口的生成：
 * 窗口并不是先创建好的，而是在事件到达时动态创建的
 * <p>
 * 分区/非分区窗口：
 * keyedWindow/nonKeyedWindow
 * <p>
 * 增量聚合函数：每一条数据到达都会先处理
 * 1）ReduceFunction#reduce() - 输入输出类型一致
 * 2）AggregateFunction#aggregate() - 输入输出类型可以不一致
 * <p>
 * 全窗口函数：不会先处理数据，只是先收集数据，窗口关闭时才进行处理，效率低
 * ProcessWindowFunction#process()
 * <p>
 * 增量聚合和全窗口函数同时使用：
 * WindowedStream#reduce(增量聚合函数ReduceFunction，全窗口函数)
 * WindowedStream#aggregate(增量聚合函数AggregateFunction,全窗口函数)
 * <p>
 * 为什么需要窗口：
 * 没有窗口的情况下，每来一条数据就输出一次，太过频繁也没有必要
 * 开窗口后，只有水位线到达窗口关闭时间时才会输出
 */
public class F02_Window {

    public static void main(String[] args) throws Exception {
        // 1、获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2、添加数据源
        SingleOutputStreamOperator<ClickEvent> clickDS = env.socketTextStream("localhost", 7777)
                .map(new MapFunction<String, ClickEvent>() {
                    @Override
                    public ClickEvent map(String value) throws Exception {
                        String[] split = value.split(",");
                        if (split.length == 3) {
                            return new ClickEvent(split[0], split[1], Long.parseLong(split[2]));
                        }
                        return null;
                    }
                }).filter(Objects::nonNull);
        // 3、数据转换
        // 4、数据输出
        clickDS.assignTimestampsAndWatermarks(WatermarkStrategy.<ClickEvent>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<ClickEvent>() {
                    @Override
                    public long extractTimestamp(ClickEvent element, long recordTimestamp) {
                        return element.timestamp;
                    }
                }))
                .keyBy(r -> r.user)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                // 增量聚合函数 reduce
                /*.reduce(new ReduceFunction<ClickEvent>() {
                    @Override
                    public ClickEvent reduce(ClickEvent value1, ClickEvent value2) throws Exception {
                        value1.url = value1.url + value2.url;
                        return value1;
                    }
                })*/
                // 增量聚合函数 aggregate
//                .aggregate(new AvgPv())
                // 增量聚合函数与全窗口函数联合使用
//                .aggregate(new UrlViewCountAgg(), new UrlViewCountResult())
                .process(new ProcessWindowFunction<ClickEvent, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<ClickEvent> elements, Collector<String> out) throws Exception {
                        for (ClickEvent element : elements) {
                            out.collect("watermark = " + context.currentWatermark() + " key = " + key + " value = " + element.toString());
                        }
                    }
                }).print();

        // 5、执行
        env.execute();
    }

    public static class AvgPv implements AggregateFunction<ClickEvent, Tuple2<HashSet<String>, Long>, Double> {
        @Override
        public Tuple2<HashSet<String>, Long> createAccumulator() {
            // 创建累加器
            return Tuple2.of(new HashSet<String>(), 0L);
        }

        @Override
        public Tuple2<HashSet<String>, Long> add(ClickEvent value, Tuple2<HashSet<String>, Long> accumulator) {
            // 属于本窗口的数据来一条累加一次，并返回累加器
            accumulator.f0.add(value.user);
            return Tuple2.of(accumulator.f0, accumulator.f1 + 1L);
        }

        @Override
        public Double getResult(Tuple2<HashSet<String>, Long> accumulator) {
            // 窗口闭合时，增量聚合结束，将计算结果发送到下游
            return (double) accumulator.f1 / accumulator.f0.size();
        }

        @Override
        public Tuple2<HashSet<String>, Long> merge(Tuple2<HashSet<String>, Long> a, Tuple2<HashSet<String>, Long> b) {
            return null;
        }
    }

    // 自定义增量聚合函数，来一条数据就加一
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

    // 自定义窗口处理函数，只需要包装窗口信息
    public static class UrlViewCountResult extends ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow> {

        @Override
        public void process(String url, Context context, Iterable<Long> elements, Collector<UrlViewCount> out) throws Exception {
            // 结合窗口信息，包装输出内容
            Long start = context.window().getStart();
            Long end = context.window().getEnd();
            // 迭代器中只有一个元素，就是增量聚合函数的计算结果
            out.collect(new UrlViewCount(url, elements.iterator().next(), start, end));
        }
    }
}
