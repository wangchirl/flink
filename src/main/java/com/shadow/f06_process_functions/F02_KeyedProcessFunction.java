package com.shadow.f06_process_functions;

import com.shadow.f00_pojo.ClickEvent;
import com.shadow.f02_source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 2、按键分区处理函数 org.apache.flink.streaming.api.functions.KeyedProcessFunction
 * keyBy() 之后的处理
 * - processElement()
 * - onTimer()
 * <p>
 * 可以注册定时器:
 * 1）处理时间定时器 ctx.timerService().registerProcessingTimeTimer()
 * 2）事件时间定时器 ctx.timerService().registerEventTimeTimer()
 * <p>
 * 获取当前水位线信息：
 * ctx.timerService().currentWatermark()
 */
public class F02_KeyedProcessFunction {
    public static void main(String[] args) throws Exception {
        // 1、获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2、添加数据源
        SingleOutputStreamOperator<ClickEvent> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<ClickEvent>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<ClickEvent>() {
                            @Override
                            public long extractTimestamp(ClickEvent element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));

        // 基于KeyedStream定义事件时间定时器
        stream.keyBy(data -> true)
                .process(new KeyedProcessFunction<Boolean, ClickEvent, String>() {
                    @Override
                    public void processElement(ClickEvent value, Context ctx, Collector<String> out) throws Exception {
                        out.collect("数据到达，时间戳为：" + ctx.timestamp());
                        out.collect("数据到达，水位线为：" + ctx.timerService().currentWatermark() + "\n -------分割线-------");
                        // 注册一个3秒后的定时器
                        ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 3 * 1000L);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect("定时器触发，触发时间：" + timestamp);
                    }
                })
                .print();
        // 3、数据转换
        // 4、数据输出
        // 5、执行
        env.execute();
    }
}
