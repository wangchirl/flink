package com.shadow.f09_table_sql;

import com.shadow.f00_pojo.ClickEvent;
import com.shadow.f02_source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 8、标量函数（Scalar Functions）：将输入的标量值转换成一个新的标量值
 * 1）自定义一个类来继承抽象类 ScalarFunction
 * 2）要求求值方法必须名字为 eval()，eval() 方法有入参和返回值
 *
 * 注册UDF：
 * StreamTableEnvironment#createTemporarySystemFunction()
 */
public class F08_Udf_ScalarFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 1. 自定义数据源，从流转换
        SingleOutputStreamOperator<ClickEvent> eventStream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<ClickEvent>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<ClickEvent>() {
                            @Override
                            public long extractTimestamp(ClickEvent element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );

        // 2. 将流转换成表
        Table eventTable = tableEnv.fromDataStream(eventStream,
                $("user"),
                $("url"),
                $("timestamp").rowtime().as("ts"));
        tableEnv.createTemporaryView("EventTable", eventTable);

        // 3. 注册自定义标量函数
        tableEnv.createTemporarySystemFunction("MyHash", MyHash.class);

        // 4. 调用UDF查询转换
        Table resultTable = tableEnv.sqlQuery("select user, MyHash(user) from EventTable");

        // 5. 输出到控制台
        tableEnv.executeSql("create table output (" +
                "uname STRING, " +
                "myhash INT ) " +
                "WITH (" +
                "'connector' = 'print')");
        resultTable.executeInsert("output");
    }

     // 自定义一个ScalarFunction
    public static class MyHash extends ScalarFunction {
        public int eval(String str){
            return str.hashCode();
        }
     }
}
