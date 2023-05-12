package com.shadow.f09_table_sql;

import com.shadow.f00_pojo.ClickEvent;
import com.shadow.f02_source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 4、时间和窗口
 * 1）设置水位线
 * 语法：WATERMARK FOR 时间字段 AS 时间字段 - INTERVAL '数字' 时间单位
 * 如：WATERMARK FOR et AS et - INTERVAL '1' SECOND -> et 字段作为事件时间，水位线延迟 1 秒
 * Table 中 $("时间字段").rowtime() 指定事件时间
 * 如：$("ts").rowtime() -> ts 字段作为事件时间
 * <p>
 * 2）窗口
 * - 滚动窗口（Tumbling Windows）：TUMBLE(TABLE 表名称, DESCRIPTOR(时间字段), INTERVAL '数字' 时间单位)
 * 如：TUMBLE(TABLE EventTable, DESCRIPTOR(ts), INTERVAL '1' HOUR) -> 基于时间字段 ts，对表 EventTable 中的数据开了大小为 1 小时的滚动窗口
 * <p>
 * - 滑动窗口（Hop Windows，跳跃窗口）：HOP(TABLE 表名称, DESCRIPTOR(时间字段), INTERVAL '数字' 时间单位（滑动步长）, INTERVAL '数字' 时间单位)（窗口大小）);
 * 如：HOP(TABLE EventTable, DESCRIPTOR(ts), INTERVAL '5' MINUTES, INTERVAL '1' HOURS)); -> 们基于时间属性 ts，在表 EventTable 上创建了大小为 1 小时的滑动窗口，每 5 分钟滑动一次
 * <p>
 * - 累积窗口（Cumulate Windows）：CUMULATE(TABLE 表名称, DESCRIPTOR(时间字段), INTERVAL '数字' 时间单位（累计周期）, INTERVAL '数字' 时间单位)（窗口大小）)
 * 如：CUMULATE(TABLE EventTable, DESCRIPTOR(ts), INTERVAL '1' HOURS, INTERVAL '1' DAYS)) -> 基于时间属性 ts，在表 EventTable 上定义了一个统计周期为 1 天、累积步长为 1 小时的累积窗口
 * <p>
 * - 会话窗口（Session Windows，目前尚未完全支持）
 * <p>
 * 3）开窗函数 OVER()
 * 格式：
 * SELECT
 * <聚合函数> OVER (
 * [PARTITION BY <字段 1>[, <字段 2>, ...]] // 可选
 * ORDER BY <时间属性字段>
 * <开窗范围>),
 * ...
 * FROM ...
 * <p>
 * 聚合函数：
 * SUM()、MAX()、MIN()、AVG()以及 COUNT()，一个特殊的聚合函数ROW_NUMBER()函数为每一行数据聚合得到一个排序之后的行号
 * 可以通过 GROUP BY 子句来指定分组的键（key），从而对数据按照某个字段做一个分组统计
 * <p>
 * 开窗范围：BETWEEN ... PRECEDING AND CURRENT ROW
 * 1. 范围间隔：RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW -> 开窗范围选择当前行之前 1 小时的数据
 * 2. 行间隔：ROWS BETWEEN 5 PRECEDING AND CURRENT ROW -> 开窗范围选择当前行之前的 5 行数据（最终聚合会包括当前行，所以一共 6 条数据）
 */
public class F04_TimeAndWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 1. 在创建表的DDL中直接定义时间属性
        String createDDL = "CREATE TABLE clickTable (" +
                " user_name STRING, " +
                " url STRING, " +
                " ts BIGINT, " +
                " et AS TO_TIMESTAMP( FROM_UNIXTIME(ts / 1000) ), " +
                " WATERMARK FOR et AS et - INTERVAL '1' SECOND " +
                ") WITH (" +
                " 'connector' = 'filesystem', " +
                " 'path' = 'input/clicks.csv', " +
                " 'format' =  'csv' " +
                ")";

        tableEnv.executeSql(createDDL);

        // 2. 在流转换成Table时定义时间属性
        SingleOutputStreamOperator<ClickEvent> clickStream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<ClickEvent>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<ClickEvent>() {
                            @Override
                            public long extractTimestamp(ClickEvent event, long l) {
                                return event.timestamp;
                            }
                        }));

        Table clickTable = tableEnv.fromDataStream(clickStream, $("user"), $("url"), $("timestamp").as("ts"),
                $("ts").rowtime());
        // 窗口表结构
        clickTable.printSchema();

        // 聚合查询转换
        // 1. 分组聚合
        Table aggTable = tableEnv.sqlQuery("SELECT user_name, COUNT(1) FROM clickTable GROUP BY user_name");

        // 2. 窗口 - 分组窗口聚合（老版本）
        Table groupWindowResultTable = tableEnv.sqlQuery("SELECT " +
                "user_name, " +
                "COUNT(1) AS cnt, " +
                "TUMBLE_END(et, INTERVAL '10' SECOND) as endT " +
                "FROM clickTable " +
                "GROUP BY " +                     // 使用窗口和用户名进行分组
                "  user_name, " +
                "  TUMBLE(et, INTERVAL '10' SECOND)" // 定义1小时滚动窗口
        );

        // 3. 窗口 - 窗口表值函数（新版本）
        // 3.1 滚动窗口
        Table tumbleWindowResultTable = tableEnv.sqlQuery("SELECT user_name, COUNT(url) AS cnt, " +
                " window_end AS endT " +
                "FROM TABLE( " +
                "  TUMBLE( TABLE clickTable, DESCRIPTOR(et), INTERVAL '10' SECOND)" +
                ") " +
                "GROUP BY user_name, window_start, window_end "
        );

        // 3.2 滑动窗口
        Table hopWindowResultTable = tableEnv.sqlQuery("SELECT user_name, COUNT(url) AS cnt, " +
                " window_end AS endT " +
                "FROM TABLE( " +
                "  HOP( TABLE clickTable, DESCRIPTOR(et), INTERVAL '5' SECOND, INTERVAL '10' SECOND)" +
                ") " +
                "GROUP BY user_name, window_start, window_end "
        );

        // 3.3 累积窗口
        Table cumulateWindowResultTable = tableEnv.sqlQuery("SELECT user_name, COUNT(url) AS cnt, " +
                " window_end AS endT " +
                "FROM TABLE( " +
                "  CUMULATE( TABLE clickTable, DESCRIPTOR(et), INTERVAL '5' SECOND, INTERVAL '10' SECOND)" +
                ") " +
                "GROUP BY user_name, window_start, window_end "
        );

        // 4. 开窗聚合
        Table overWindowResultTable = tableEnv.sqlQuery("SELECT user_name, " +
                " avg(ts) OVER (" +
                "   PARTITION BY user_name " +
                "   ORDER BY et " +
                "   ROWS BETWEEN 3 PRECEDING AND CURRENT ROW" +
                ") AS avg_ts " +
                "FROM clickTable");

        // 结果表转换成流打印输出
//        tableEnv.toChangelogStream(aggTable).print("agg: ");
//        tableEnv.toDataStream(groupWindowResultTable).print("group window: ");
//        tableEnv.toDataStream(tumbleWindowResultTable).print("tumble window: ");
//        tableEnv.toDataStream(hopWindowResultTable).print("hop window: ");
//        tableEnv.toDataStream(cumulateWindowResultTable).print("cumulate window: ");
        tableEnv.toDataStream(overWindowResultTable).print("over window: ");

        env.execute();
    }
}
