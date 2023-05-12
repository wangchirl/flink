package com.shadow.f07_multiple_stream;

import com.atguigu.chapter05.common.Event;
import com.shadow.f00_pojo.ClickEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 2、合流
 * 基本合流：
 * 2.1 union：可以联合多条流，要求每条流的数据类型必须一致
 * 2.2 connect：只能连接两条流，但不要求流的数据类型必须一致
 * <p>
 * 基于时间的合流：
 * 2.3 join：两条流中关联key一样的数据会进入到处理方法，存在笛卡尔积，类似SQL的 inner join
 * 标准格式：
 * stream1.join(stream2)
 * .where()
 * .equal()
 * .window()
 * .apply(JoinFunction)
 * <p>
 * 2.4 intervalJoin：针对一条流的每一个key设置一个上下限，范围内能匹配的会进入到处理方法
 * 标准格式：
 * stream1.keyBy()
 * .intervalJoin()
 * .between()
 * .process(ProcessJoinFunction)
 * <p>
 * 2.5 coGroup：窗口同组联结，同 join，不仅可以实现类似SQL的inner join，还可以实现left join、right join、full outer join
 * 标准格式：
 * stream1.join(stream2)
 * .where()
 * .equal()
 * .window()
 * .apply(CoGroupFunction)
 */
public class F02_MergeFlow {
    public static void main(String[] args) throws Exception {
        // 1、union
        union();

        // 2、connect
        connect();

        // 3、window join
        join();

        // 4、interval join
        intervalJoin();

        // 5、coGroup : connect + group by
        coGroup();

    }

    private static void union() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<ClickEvent> stream1 = env.socketTextStream("hadoop102", 7777)
                .map(data -> {
                    String[] field = data.split(",");
                    return new ClickEvent(field[0].trim(), field[1].trim(), Long.valueOf(field[2].trim()));
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<ClickEvent>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<ClickEvent>() {
                            @Override
                            public long extractTimestamp(ClickEvent element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );
        stream1.print("stream1");

        SingleOutputStreamOperator<ClickEvent> stream2 = env.socketTextStream("hadoop103", 7777)
                .map(data -> {
                    String[] field = data.split(",");
                    return new ClickEvent(field[0].trim(), field[1].trim(), Long.valueOf(field[2].trim()));
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<ClickEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<ClickEvent>() {
                            @Override
                            public long extractTimestamp(ClickEvent element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );

        stream2.print("stream2");

        // 合并两条流
        stream1.union(stream2)
                .process(new ProcessFunction<ClickEvent, String>() {
                    @Override
                    public void processElement(ClickEvent value, Context ctx, Collector<String> out) throws Exception {
                        out.collect("水位线：" + ctx.timerService().currentWatermark());
                    }
                })
                .print();

        env.execute();
    }

    private static void connect() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Integer> stream1 = env.fromElements(1, 2, 3);
        DataStream<Long> stream2 = env.fromElements(1L, 2L, 3L);

        ConnectedStreams<Integer, Long> connectedStreams = stream1.connect(stream2);
        SingleOutputStreamOperator<String> result = connectedStreams.map(new CoMapFunction<Integer, Long, String>() {
            @Override
            public String map1(Integer value) {
                return "Integer: " + value;
            }

            @Override
            public String map2(Long value) {
                return "Long: " + value;
            }
        });

        result.print();

        env.execute();
    }

    private static void join() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Tuple2<String, Long>> stream1 = env
                .fromElements(
                        Tuple2.of("a", 1000L),
                        Tuple2.of("b", 1000L),
                        Tuple2.of("a", 2000L),
                        Tuple2.of("b", 2000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple2<String, Long>>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                            @Override
                                            public long extractTimestamp(Tuple2<String, Long> stringLongTuple2, long l) {
                                                return stringLongTuple2.f1;
                                            }
                                        }
                                )
                );

        DataStream<Tuple2<String, Long>> stream2 = env
                .fromElements(
                        Tuple2.of("a", 3000L),
                        Tuple2.of("b", 3000L),
                        Tuple2.of("a", 4000L),
                        Tuple2.of("b", 4000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple2<String, Long>>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                            @Override
                                            public long extractTimestamp(Tuple2<String, Long> stringLongTuple2, long l) {
                                                return stringLongTuple2.f1;
                                            }
                                        }
                                )
                );

        stream1.join(stream2)
                .where(r -> r.f0)
                .equalTo(r -> r.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new JoinFunction<Tuple2<String, Long>, Tuple2<String, Long>, String>() {
                    @Override
                    public String join(Tuple2<String, Long> left, Tuple2<String, Long> right) throws Exception {
                        return left + "=>" + right;
                    }
                })
                .print();

        env.execute();
    }


    private static void intervalJoin() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple3<String, String, Long>> orderStream = env.fromElements(
                Tuple3.of("Mary", "order-1", 5000L),
                Tuple3.of("Alice", "order-2", 5000L),
                Tuple3.of("Bob", "order-3", 20000L),
                Tuple3.of("Alice", "order-4", 20000L),
                Tuple3.of("Cary", "order-5", 51000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                        return element.f2;
                    }
                })
        );

        SingleOutputStreamOperator<Event> clickStream = env.fromElements(
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Alice", "./prod?id=200", 3500L),
                new Event("Bob", "./prod?id=2", 2500L),
                new Event("Alice", "./prod?id=300", 36000L),
                new Event("Bob", "./home", 30000L),
                new Event("Bob", "./prod?id=1", 23000L),
                new Event("Bob", "./prod?id=3", 33000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timestamp;
                    }
                })
        );

        orderStream.keyBy(data -> data.f0)
                .intervalJoin(clickStream.keyBy(data -> data.user))
                .between(Time.seconds(-5), Time.seconds(10))
                .process(new ProcessJoinFunction<Tuple3<String, String, Long>, Event, String>() {
                    @Override
                    public void processElement(Tuple3<String, String, Long> left, Event right, Context ctx, Collector<String> out) throws Exception {
                        out.collect(right + " => " + left);
                    }
                })
                .print();

        env.execute();
    }

    private static void coGroup() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Tuple2<String, Long>> stream1 = env
                .fromElements(
                        Tuple2.of("a", 1000L),
                        Tuple2.of("b", 1000L),
                        Tuple2.of("a", 2000L),
                        Tuple2.of("b", 2000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple2<String, Long>>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                            @Override
                                            public long extractTimestamp(Tuple2<String, Long> stringLongTuple2, long l) {
                                                return stringLongTuple2.f1;
                                            }
                                        }
                                )
                );

        DataStream<Tuple2<String, Long>> stream2 = env
                .fromElements(
                        Tuple2.of("a", 3000L),
                        Tuple2.of("b", 3000L),
                        Tuple2.of("a", 4000L),
                        Tuple2.of("b", 4000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple2<String, Long>>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                            @Override
                                            public long extractTimestamp(Tuple2<String, Long> stringLongTuple2, long l) {
                                                return stringLongTuple2.f1;
                                            }
                                        }
                                )
                );

        stream1.coGroup(stream2)
                .where(r -> r.f0)
                .equalTo(r -> r.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new CoGroupFunction<Tuple2<String, Long>, Tuple2<String, Long>, String>() {
                    @Override
                    public void coGroup(Iterable<Tuple2<String, Long>> iter1, Iterable<Tuple2<String, Long>> iter2, Collector<String> collector) throws Exception {
                        collector.collect(iter1 + "=>" + iter2);
                    }
                })
                .print();

        env.execute();
    }
}
