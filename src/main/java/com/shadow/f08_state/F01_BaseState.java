package com.shadow.f08_state;

import com.shadow.f00_pojo.ClickEvent;
import com.shadow.f02_source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 1、状态编程
 * 为什么需要状态：输出结果需要依赖历史数据
 * <p>
 * 按键分区状态类型：
 * - 值状态 ValueState
 * - 列表状态 ListState
 * - 映射状态 MapState
 * - 规约状态 ReducingState
 * - 聚合状态 AggregateState
 * <p>
 * 算子状态类型：常见于 source sink 中使用
 * - 列表状态 ListState
 * - 联合列表状态 UnionListState (k-v模型，类似MapState)
 * - 广播状态 BroadcastState
 * <p>
 * 状态的创建：
 * 一般情况在富函数的生命周期方法 open 中进行创建
 */
public class F01_BaseState {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<ClickEvent> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<ClickEvent>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<ClickEvent>() {
                            @Override
                            public long extractTimestamp(ClickEvent element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );

        stream.keyBy(data -> data.user)
                .flatMap(new MyFlatMap())
                .print();


        env.execute();
    }

    // 实现自定义的FlatMapFunction，用于Keyed State测试
    public static class MyFlatMap extends RichFlatMapFunction<ClickEvent, String> {
        // 定义状态
        ValueState<ClickEvent> myValueState;
        ListState<ClickEvent> myListState;
        MapState<String, Long> myMapState;
        ReducingState<ClickEvent> myReducingState;
        AggregatingState<ClickEvent, String> myAggregatingState;

        // 增加一个本地变量进行对比
        Long count = 0L;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<ClickEvent> valueStateDescriptor = new ValueStateDescriptor<>("my-state", ClickEvent.class);
            myValueState = getRuntimeContext().getState(valueStateDescriptor);

            myListState = getRuntimeContext().getListState(new ListStateDescriptor<ClickEvent>("my-list", ClickEvent.class));
            myMapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Long>("my-map", String.class, Long.class));

            myReducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<ClickEvent>("my-reduce",
                    new ReduceFunction<ClickEvent>() {
                        @Override
                        public ClickEvent reduce(ClickEvent value1, ClickEvent value2) throws Exception {
                            return new ClickEvent(value1.user, value1.url, value2.timestamp);
                        }
                    }
                    , ClickEvent.class));

            myAggregatingState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<ClickEvent, Long, String>("my-agg",
                    new AggregateFunction<ClickEvent, Long, String>() {
                        @Override
                        public Long createAccumulator() {
                            return 0L;
                        }

                        @Override
                        public Long add(ClickEvent value, Long accmulator) {
                            return accmulator + 1;
                        }

                        @Override
                        public String getResult(Long accumulator) {
                            return "count: " + accumulator;
                        }

                        @Override
                        public Long merge(Long a, Long b) {
                            return a + b;
                        }
                    }
                    , Long.class));

            // 配置状态的TTL
            StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.hours(1))
                    .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                    .build();

            valueStateDescriptor.enableTimeToLive(ttlConfig);
        }

        @Override
        public void flatMap(ClickEvent value, Collector<String> out) throws Exception {
            // 访问和更新状态
            System.out.println(myValueState.value());
            myValueState.update(value);
            System.out.println( "my value: " + myValueState.value() );

            myListState.add(value);

            myMapState.put(value.user, myMapState.get(value.user) == null? 1: myMapState.get(value.user) + 1);
            System.out.println( "my map value: " + myMapState.get(value.user) );

            myReducingState.add(value);
            System.out.println( "my reducing value: " + myReducingState.get() );

            myAggregatingState.add(value);
            System.out.println( "my agg value: " + myAggregatingState.get() );

            count ++;
            System.out.println("count: " + count);
        }
    }
}
