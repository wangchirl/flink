package com.shadow.f09_table_sql;

import com.atguigu.chapter05.common.Event;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 1、Flink Table & SQL 基础
 * 1）流处理执行环境 org.apache.flink.table.api.bridge.java.StreamTableEnvironment
 * - 从流执行环境中创建 StreamTableEnvironment.create(StreamExecutionEnvironment)
 * - 使用 EnvironmentSettings 创建
 * EnvironmentSettings settings = EnvironmentSettings.newInstance()
 * .inStreamingMode()
 * .useOldPlanner()
 * .build();
 * TableEnvironment tableEnv1 = TableEnvironment.create(settings);
 * <p>
 * 2）批处理执行环境 org.apache.flink.table.api.TableEnvironment【不怎么使用】
 * - 从批处理执行环境创建 BatchTableEnvironment.create(ExecutionEnvironment)
 * - 使用 EnvironmentSettings 创建
 * EnvironmentSettings settings3 = EnvironmentSettings.newInstance()
 * .inBatchMode()
 * .useBlinkPlanner()
 * .build();
 * TableEnvironment tableEnv3 = TableEnvironment.create(settings);
 */
public class F01_BaseTableSql {
    public static void main(String[] args) throws Exception {
        // 获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. 读取数据源
        SingleOutputStreamOperator<Event> eventStream = env
                .fromElements(
                        new Event("Alice", "./home", 1000L),
                        new Event("Bob", "./cart", 1000L),
                        new Event("Cary", "./home", 60 * 1000L),
                        new Event("Bob", "./prod?id=3", 90 * 1000L),
                        new Event("Alice", "./prod?id=7", 105 * 1000L)
                );

        // 2. 获取表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 3. 将数据流转换成表
        Table eventTable = tableEnv.fromDataStream(eventStream);

        // 4. 用执行SQL 的方式提取数据
        Table resultTable1 = tableEnv.sqlQuery("select url, user from " + eventTable);

        // 5. 基于Table直接转换
        Table resultTable2 = eventTable.select($("user"), $("url"))
                .where($("user").isEqual("Alice"));

        // 6. 将表转换成数据流，打印输出
        tableEnv.toDataStream(resultTable1).print("result1");
        tableEnv.toDataStream(resultTable2).print("result2");

        // 执行程序
        env.execute();
    }
}

