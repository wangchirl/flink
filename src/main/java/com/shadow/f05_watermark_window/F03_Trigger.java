package com.shadow.f05_watermark_window;

import com.shadow.f00_pojo.ClickEvent;
import com.shadow.f00_pojo.UrlViewCount;
import com.shadow.f02_source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 3、触发器与移除器
 * 触发器：org.apache.flink.streaming.api.windowing.triggers.Trigger
 * - onElement() 窗口中没个元素到来都会调用
 * - onProcessingTime() 当注册的处理时间定时器触发时调用
 * - onEventTime() 当注册的时间事件定时器触发时调用
 * - clear() 当窗口关闭销毁时调用，一般用来清除自定义的状态信息
 * <p>
 * org.apache.flink.streaming.api.windowing.triggers.TriggerResult
 * - CONTINUE 什么都不做
 * - FIRE_AND_PURGE 触发计数输出结果并销毁窗口
 * - FIRE 触发计数输出结果
 * - PURGE 清除窗口中所有数据并销毁窗口
 * <p>
 * 默认实现：
 * EventTimeTrigger
 * ProcessingTimeTrigger
 * CountTrigger
 * <p>
 * 移除器：org.apache.flink.streaming.api.windowing.evictors.Evictor
 * - evictBefore() 执行窗口函数之前移除数据
 * - evictAfter() 执行窗口函数之后移除数据
 * 默认实现的移除器是在执行窗口函数之前移除数据
 * <p>
 * 默认实现：
 * TimeEvictor
 * CountEvictor
 */
public class F03_Trigger {
    public static void main(String[] args) throws Exception {
        // 1、获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2、添加数据源
        // 3、数据转换
        // 4、数据输出
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
                .keyBy(r -> r.url)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .trigger(new MyTrigger())
                .evictor(TimeEvictor.of(Time.seconds(2)))
                .process(new WindowResult())
                .print();
        // 5、执行
        env.execute();
    }

    public static class MyTrigger extends Trigger<ClickEvent, TimeWindow> {
        @Override
        public TriggerResult onElement(ClickEvent event, long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            ValueState<Boolean> isFirstEvent = triggerContext.getPartitionedState(
                    new ValueStateDescriptor<Boolean>("first-event", Types.BOOLEAN)
            );
            if (isFirstEvent.value() == null) {
                for (long i = timeWindow.getStart(); i < timeWindow.getEnd(); i = i + 1000L) {
                    triggerContext.registerEventTimeTimer(i);
                }
                isFirstEvent.update(true);
            }
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            return TriggerResult.FIRE;
        }

        @Override
        public TriggerResult onProcessingTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            ValueState<Boolean> isFirstEvent = triggerContext.getPartitionedState(
                    new ValueStateDescriptor<Boolean>("first-event", Types.BOOLEAN)
            );
            isFirstEvent.clear();
        }
    }

    public static class WindowResult extends ProcessWindowFunction<ClickEvent, UrlViewCount, String, TimeWindow> {
        @Override
        public void process(String s, Context context, Iterable<ClickEvent> iterable, Collector<UrlViewCount> collector) throws Exception {
            collector.collect(
                    new UrlViewCount(
                            s,
                            // 获取迭代器中的元素个数
                            iterable.spliterator().getExactSizeIfKnown(),
                            context.window().getStart(),
                            context.window().getEnd()
                    )
            );
        }
    }
}
