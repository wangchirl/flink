package com.shadow.f04_sink;

import com.shadow.f00_pojo.ClickEvent;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * 4、addSink - 输出到 Redis
 * RedisSink(JFlinkJedisConfigBase,RedisMapper)
 * - JFlinkJedisConfigBase: Jedis 的连接配置
 * - RedisMapper: Redis 映射类接口，说明怎样将数据转换成可以写入 Redis 的类型
 * <p>
 * 下载 Redis：https://download.redis.io/releases/?_gl=1*t84tm0*_ga*MTA3MDEyMTE1MC4xNjE5MjU1OTQx*_ga_8BKGRQKRPV*MTY4MDUzMzQzMy4xLjEuMTY4MDUzMzU0MS40My4wLjA.
 * 启动 Redis：./bin/redis-server
 * 启动 Redis客户端：./bin/redis-cli
 * 查看 hget clicks shadow
 */
public class T04_RedisSink {

    public static void main(String[] args) throws Exception {
        // 1、获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2、添加数据源
        DataStreamSource<ClickEvent> clickDS = env.fromElements(
                new ClickEvent("shadow", "./index", 1000L),
                new ClickEvent("oracle", "./index", 2000L),
                new ClickEvent("guess", "./cart", 3000L),
                new ClickEvent("great", "./home", 5000L)
        );

        // 3、数据转换
        // 4、数据输出
        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder().setHost("localhost")
                .setPort(6379)
                .build();
        clickDS.addSink(new RedisSink<>(config, new RedisMapper<ClickEvent>() {
            @Override
            public RedisCommandDescription getCommandDescription() {
                return new RedisCommandDescription(RedisCommand.HSET, "clicks");
            }

            @Override
            public String getKeyFromData(ClickEvent event) {
                return event.user;
            }

            @Override
            public String getValueFromData(ClickEvent event) {
                return event.url;
            }
        }));

        // 5、执行
        env.execute();
    }
}
