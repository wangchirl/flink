package com.shadow.f04_sink;

import com.shadow.f00_pojo.ClickEvent;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 3、addSink 输出到 Mysql
 * 创建表
 * CREATE TABLE `events` (
 * `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
 * `user` varchar(255) DEFAULT '',
 * `url` varchar(255) DEFAULT '',
 * PRIMARY KEY (`id`)
 * ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
 * <p>
 * JdbcSink.sink()
 */
public class T03_JdbcSink {
    public static void main(String[] args) throws Exception {
        // 1、获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2、source
        DataStreamSource<ClickEvent> clickDS = env.fromElements(
                new ClickEvent("shadow", "./index", 1000L),
                new ClickEvent("oracle", "./home", 2000L),
                new ClickEvent("shadow", "./cart", 3000L),
                new ClickEvent("oracle", "./index", 4000L)
        );

        // 3、transformation
        // 4、sink
        clickDS.addSink(
                JdbcSink.sink(
                        "INSERT INTO events(user, url) VALUES(?, ?)",
                        (preparedStatement, event) -> {
                            preparedStatement.setString(1, event.user);
                            preparedStatement.setString(2, event.url);
                        },
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl("jdbc:mysql://localhost:3306/demo")
                                .withDriverName("com.mysql.jdbc.Driver")
                                .withUsername("root")
                                .withPassword("root")
                                .build()
                )
        );

        // 5、execute
        env.execute();
    }
}
