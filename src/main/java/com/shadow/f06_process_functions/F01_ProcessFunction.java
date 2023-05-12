package com.shadow.f06_process_functions;

import com.shadow.f00_pojo.ClickEvent;
import com.shadow.f02_source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 1、基本处理函数 org.apache.flink.streaming.api.functions.ProcessFunction
 * - processElement() 流中的每个元素都会调用一次，处理元素的核心逻辑
 * - onTimer() 注册的定时器触发时的处理逻辑 （注：只有按键分区流才能设置定时器操作）
 */
public class F01_ProcessFunction {
    public static void main(String[] args) throws Exception {
        // 1、获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2、添加数据源
        env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<ClickEvent>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<ClickEvent>() {
                                    @Override
                                    public long extractTimestamp(ClickEvent event, long l) {
                                        return event.timestamp;
                                    }
                                })
                )
                .process(new ProcessFunction<ClickEvent, String>() {
                    @Override
                    public void processElement(ClickEvent value, Context ctx, Collector<String> out) throws Exception {
                        if (value.user.equals("shadow")) {
                            out.collect(value.user);
                        } else if (value.user.equals("oracle")) {
                            out.collect(value.user);
                            out.collect(value.user);
                        }
                        System.out.println(ctx.timerService().currentWatermark());
                    }
                })
                .print();
        // 3、数据转换
        // 4、数据输出

        // 5、执行
        env.execute();
    }
}
