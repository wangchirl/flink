package com.shadow.f09_table_sql;


import com.shadow.f00_pojo.ClickEvent;
import com.shadow.f02_source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 9、表函数（Table Functions）：将标量值转换成一个或多个新的行数据，也就是扩展成一个表
 * 1）自定义类来继承抽象类 TableFunction
 * 2）实现的也是一个名为 eval 的求值方法，eval()方法没有返回类型，内部也没有 return语句，是通过调用 collect()方法来发送想要输出的行数据的
 */
public class F09_Udf_TableFunction {
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

        // 3. 注册自定义表函数
        tableEnv.createTemporarySystemFunction("MySplit", MySplit.class);

        // 4. 调用UDF查询转换
        Table resultTable = tableEnv.sqlQuery("select user, url, word, length " +
                "from EventTable, LATERAL TABLE( MySplit(url) ) AS T(word, length)");

        // 5. 输出到控制台
        tableEnv.executeSql("create table output (" +
                "uname STRING, " +
                "url STRING, " +
                "word STRING, " +
                "length INT) " +
                "WITH (" +
                "'connector' = 'print')");
        resultTable.executeInsert("output");
    }

    // 自定义一个TableFunction，注意有泛型，这里输出的是两个字段，二元组
    public static class MySplit extends TableFunction<Tuple2<String, Integer>>{
        public void eval(String str){
            String[] fields = str.split("\\?");    // 转义问号，以及反斜杠本身
            for (String field : fields){
                collect(Tuple2.of(field, field.length()));
            }
        }
    }
}
