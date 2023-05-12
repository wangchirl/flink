package com.shadow.f09_table_sql;

import com.shadow.f00_pojo.ClickEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 5、累计窗口测试
 */
public class F05_CumulateWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取数据源，并分配时间戳、生成水位线
        SingleOutputStreamOperator<ClickEvent> eventStream = env
                .fromElements(
                        new ClickEvent("Alice", "./home", 1000L),
                        new ClickEvent("Bob", "./cart", 1000L),
                        new ClickEvent("Alice", "./prod?id=1", 25 * 60 * 1000L),
                        new ClickEvent("Alice", "./prod?id=4", 55 * 60 * 1000L),
                        new ClickEvent("Bob", "./prod?id=5", 3600 * 1000L + 60 * 1000L),
                        new ClickEvent("Cary", "./home", 3600 * 1000L + 30 * 60 * 1000L),
                        new ClickEvent("Cary", "./prod?id=7", 3600 * 1000L + 59 * 60 * 1000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<ClickEvent>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<ClickEvent>() {
                                    @Override
                                    public long extractTimestamp(ClickEvent element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                })
                );

        // 创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 将数据流转换成表，并指定时间属性
        Table eventTable = tableEnv.fromDataStream(
                eventStream,
                $("user"),
                $("url"),
                $("timestamp").rowtime().as("ts")
        );

        // 为方便在SQL中引用，在环境中注册表EventTable
        tableEnv.createTemporaryView("EventTable", eventTable);

        // 设置累积窗口，执行SQL统计查询
        Table result = tableEnv
                .sqlQuery(
                        "SELECT " +
                                "user, " +
                                "window_end AS endT, " +
                                "COUNT(url) AS cnt " +
                                "FROM TABLE( " +
                                "CUMULATE( TABLE EventTable, " +    // 定义累积窗口
                                "DESCRIPTOR(ts), " +
                                "INTERVAL '30' MINUTE, " +
                                "INTERVAL '1' HOUR)) " +
                                "GROUP BY user, window_start, window_end "
                );

        tableEnv.toDataStream(result).print();

        env.execute();
    }
}

