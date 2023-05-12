## Flink

### 一、执行环境

```java
/**
 * 1、批处理 API 执行环境
 * ExecutionEnvironment
 * 由于Flink对批流进行了统一，后续直接使用流处理执行环境
 */
```

```java
/**
 * 2、流处理执行环境
 * 2.1）StreamExecutionEnvironment
 * 1. getExecutionEnvironment() -> 自行决定该返回什么样的运行环境
 * 2. createLocalEnvironment() -> 返回一个本地执行环境
 * 3. createRemoteEnvironment(ip,port,jarpath) -> 返回集群执行环境
 * <p>
 * 2.2）由于流处理API进行了批流统一，可以指定执行模式：
 * 1. setRuntimeMode(RuntimeExecutionMode)
 * RuntimeExecutionMode:
 * STREAMING -> 默认流处理
 * BATCH -> 批处理
 * AUTOMATIC -> 自动判断
 * 2. 命令行指定: bin/flink run -Dexecution.runtime-mode=BATCH
 * <p>
 * 2.3）其他全局设置
 * 1.setParallelism() 设置并行读
 * 2.disableOperatorChaining() 全局禁用算子链
 * 3.setStateBackend() 设置状态后端
 * 4.setRestartStrategy() 设置重启策略
 */
```

```java
/**
 * 3、流处理
 * 1）Windows：
 * https://eternallybored.org/misc/netcat/
 * cmd -> nc.exe -> nc -lp 7777
 * 2）Linux：
 * nc -lk 7777
 */
```

### 二、源算子

```java
/**
 * 1、从集合读取数据
 * env.fromCollection()
 */
```

```java
/**
 * 2、直接构建数据
 * fromElements()
 */
```

```java
/**
 * 3、从文件读取数据：
 * env.readTextFile()
 * 1. 参数可以是目录，也可以是文件
 * 2. 路径可以是相对路径，也可以是绝对路径
 * 3. 相对路径是从系统属性 user.dir 获取路径: idea 下是 project 的根目录, standalone 模式下是集群节点根目录
 * 4. 也可以从 hdfs 目录下读取, 使用路径 hdfs://..., 由于 Flink 没有提供 hadoop 相关依赖,需要 pom 中添加相关依赖
 * <dependency>
 *  <groupId>org.apache.hadoop</groupId>
 *  <artifactId>hadoop-client</artifactId>
 *  <version>2.7.5</version>
 *  <scope>provided</scope>
 * </dependency>
 */
```

```java
/**
 * 4、Socket 读取数据
 * env.socketTextStream()
 * 启动 socket 服务：
 * WINDOWS:
 * 1.下载 netcat 服务 https://eternallybored.org/misc/netcat/
 * 2.cmd -> nc.exe -lp 7777
 * LINUX:
 * nc -lk 7777
 */
```

```java
/**
 * 5、Kafka 消费数据
 * 1. 启动 ZK
 * 1.1 下载 ZK -> https://www.apache.org/dyn/closer.lua/zookeeper/zookeeper-3.7.1/apache-zookeeper-3.7.1-bin.tar.gz
 * ./bin/zkServer.sh
 * 2. 启动 Kafka
 * ./bin/kafka-server-start.sh start config/properties
 */
```

```java
/**
 * 6、Pulsar 读取数据
 * https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/connectors/datastream/pulsar/
 * <p>
 * 6.1 启动 zk
 * 6.2 启动 bookie
 * 6.3 启动 bookkeeper
 */
```

```java
/**
 * 7、自定义数据源
 * 7.1 实现接口 SourceFunction
 * run()方法：使用运行时上下文对象（SourceContext）向下游发送数据
 * cancel()方法：通过标识位控制退出循环，来达到中断数据源的效果
 * note：其 SourceFunction 的并行度只能是 1，否则报错
 * <p>
 * 7.2 实现接口 ParallelSourceFunction
 * 支持并行度设置
 */
```

### 三、转换算子

```java
/**
 * 1、map() 映射 -> 可以改变输入的类型
 *
 * @see org.apache.flink.api.common.functions.MapFunction
 * - map 转换方法
 * @see org.apache.flink.api.common.functions.RichMapFunction
 * - map 转换方法
 * - open 生命周期开始方法
 * - close 生命周期结束方法
 * - getRuntimeContext 获取运行上下文方法
 */
```

```java
/**
 * 2、filter() 过滤 -> 不会改变输入的类型
 *
 * @see org.apache.flink.api.common.functions.FilterFunction
 * - filter 过滤方法
 * @see org.apache.flink.api.common.functions.RichFilterFunction
 * - filter 过滤方法
 * - open 生命周期开始方法
 * - close 生命周期结束方法
 * - getRuntimeContext 获取运行上下文方法
 */
```

```java
/**
 * 3、flatMap() 扁平映射 -> 可以改变输入的类型
 * 可同时满足 map 和 filter 的功能
 *
 * @see org.apache.flink.api.common.functions.FlatMapFunction
 * - flatMap 处理方法
 * @see org.apache.flink.api.common.functions.RichFlatMapFunction
 * - flatMap 处理方法
 * - open 生命周期开始方法
 * - close 生命周期结束方法
 * - getRuntimeContext 获取运行上下文方法
 */
```

```java
/**
 * 4、简单聚合 keyBy 之后基于 KeyedStream
 * - sum()：在输入流上，对指定的字段做叠加求和的操作
 * - min()：在输入流上，对指定的字段求最小值【其他值为第一条记录的】，如果想得到最小记录的整个数据使用 minBy
 * - max()：在输入流上，对指定的字段求最大值【其他值为第一条记录的】，如果想得到最大记录的整个数据使用 maxBy
 * - minBy()：与 min()类似，在输入流上针对指定字段求最小值。不同的是，min()只计
 * 算指定字段的最小值，其他字段会保留最初第一个数据的值；而 minBy()则会返回包含字段最小值的整条数据
 * - maxBy()：与 max()类似，在输入流上针对指定字段求最大值。两者区别与min()/minBy()完全一致
 */
```

```java
/**
 * 5、reduce 规约聚合 -> 不会改变输入的类型
 * 是一个一般化的聚合统计操作
 *
 * @see org.apache.flink.api.common.functions.ReduceFunction
 * 内部会维护一个初始值为空的累加器，累加器的类型和输入元素的类型相同
 * 当第一条元素到来时，累加器的值更新为第一条元素的值，当新的元素到来时
 * 新元素会和累加器进行累加操作，这里的累加操作就是 reduce 函数定义的运算规则
 * 然后将更新以后的累加器的值向下游输出
 * @see org.apache.flink.api.common.functions.RichReduceFunction
 * - reduce 规约方法
 * - open 生命周期开始方法
 * - close 生命周期结束方法
 * - getRuntimeContext 获取运行上下文方法
 * <p>
 * 因为状态不会清空，所以我们需要将 reduce 算子作用在一个有限 key 的流上
 */
```

```java
/**
 * 6、User Definition Function (UDF) 用户自定义函数
 * 6.1 函数类 ：MapFunction、FilterFunction、FlatMapFunction、ReduceFunction...
 * 6.2 匿名函数（Lambda）
 * 6.3 富函数类：RichMapFunction、RichFilterFunction、RichFlatMapFunction、RichReduceFunction...
 */
```

```java
/**
 * 7、物理分区
 * - shuffle() 随机分区（洗牌）
 * - rebalance() 轮询分区（Round-Robin）
 * - rescale() 重缩放分区（rescale）
 * 重缩放分区和轮询分区非常相似。当调用 rescale()方法时，其实底层也是使用 Round-Robin算法进行轮询，但是只会将数据轮询发送到下游并行任务的一部分中
 * rebalance 的方式是每个发牌人都面向所有人发牌
 * - broadcast() 广播（broadcast）数据会在不同的分区都保留一份，可能进行重复处理
 * <p>
 * - global() 全局分区（global）将所有的输入流数据都发送到下游算子的第一个并行子任务中去。这就相当于强行让下游任务并行度变成了 1
 * <p>
 * - partitionCustom(Partitioner,KeySelector) 自定义分区
 */
```

### 四、输出算子

```java
/**
 * 1、addSink -> 输出到文件
 * 1.1 writeAsText() - 过期方法，直接写出到文件
 * 1.2 writeAsCsv() - 过期方法，直接写出到 csv 文件，要求类型必须是 Tuple 类型
 * 1.3 StreamingFileSink -> 推荐方法
 * - forRowFormat - 行编码
 * - forBulkFormat - 批量编码
 */
```

```java
/**
 * 2、addSink 输出到 Kafka
 * 2.1 FlinkKafkaProducer - 老版本
 * - addSink(FlinkKafkaProducer)
 * 2.2 KafkaSink - 新版本
 * - sinkTo(KafkaSink)
 * <p>
 * 启动 ZK
 * 下载 ZK -> https://www.apache.org/dyn/closer.lua/zookeeper/zookeeper-3.7.1/apache-zookeeper-3.7.1-bin.tar.gz
 * ./bin/zkServer.sh
 * 启动 Kafka
 * ./bin/kafka-server-start.sh start config/properties
 * 启动消费者
 * ./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic clicks
 */
```

```java
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
```

```java
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
```

```java
/**
 * 5、addSink 输出到 Es
 * 启动 Es，启动前修改 vm 内存 -Xmx512m -Xms512m
 * WINDOWS: cmd -> elasticsearch.bat
 * LINUX: ./elasticsearch
 * <p>
 * 查看索引
 * curl http://localhost:9200/_cat/indices
 * 查看clicks索引mapping
 * curl http://localhost:9200/clicks/_mapping
 * 查看数据
 * curl http://localhost:9200/clicks/_search
 */
```

### 五、水位线与窗口

```java
/**
 * 时间分类：
 * 1）处理时间
 * 2）事件时间（重点）
 * 3）摄入时间
 * <p>
 * 1、水位线：watermark -> 描述数据流中时间进展的逻辑时钟
 * 表示：当前水位线时间 t 之前的数据都到齐了
 * 通过 assignTimestampsAndWatermarks() 指定水位线及事件时间
 * 默认水位线实现：
 * WatermarkStrategy.forMonotonousTimestamps() - 有序
 * WatermarkStrategy.forBoundedOutOfOrderness(Duration) - 无序
 * WatermarkStrategy.noWatermarks() - 不生成水位线
 * 注意关注：事件时间 eventTime
 * <p>
 * 水位线生成方式：
 * 1）按每条数据生成 - （太频繁）
 * 2）周期生成 - （默认200ms生成一条水位线）
 * 通过 WatermarkStrategy.withTimestampAssigner() 指定事件时间生成水位线
 * <p>
 * 水位线延迟：
 * 针对无序流，先产生的数据可能后被处理，水位线根据数据时间生成，水位线时间不能倒流
 * 可针对无序流设置水位线延迟
 * <p>
 * 自定义水位线：
 * 1）实现 WatermarkStrategy 接口，重写 createTimestampAssigner 方法
 * 2）创建 WatermarkGenerator 参考默认实现 BoundedOutOfOrdernessWatermarks
 * <p>
 * 设置水位线方式：
 * 1）Source - SourceContext#collectWithTimestamp() 、SourceContext#emitWatermark()
 * 2）DataStream - assignTimestampsAndWatermarks() - 更灵活
 * <p>
 * 水位线的默认计算公式：
 * 水位线 = 观察到的最大事件时间 – 最大延迟时间 – 1 毫秒
 */
```

```java
/**
 * 2、窗口 Window
 * 窗口分类：
 * 1）时间窗口（滚动、滑动、会话，全局） - 分处理时间窗口和事件时间窗口
 * TumblingEventTimeWindows/TumblingProcessingTimeWindows、SlidingEventTimeWindows/SlidingProcessingTimeWindows
 * 2）计数窗口（滚动、滑动，全局） - 底层都是基于全局窗口实现
 * <p>
 * 窗口的生成：
 * 窗口并不是先创建好的，而是在事件到达时动态创建的
 * <p>
 * 分区/非分区窗口：
 * keyedWindow/nonKeyedWindow
 * <p>
 * 增量聚合函数：每一条数据到达都会先处理
 * 1）ReduceFunction#reduce() - 输入输出类型一致
 * 2）AggregateFunction#aggregate() - 输入输出类型可以不一致
 * <p>
 * 全窗口函数：不会先处理数据，只是先收集数据，窗口关闭时才进行处理，效率低
 * ProcessWindowFunction#process()
 * <p>
 * 增量聚合和全窗口函数同时使用：
 * WindowedStream#reduce(增量聚合函数ReduceFunction，全窗口函数)
 * WindowedStream#aggregate(增量聚合函数AggregateFunction,全窗口函数)
 * <p>
 * 为什么需要窗口：
 * 没有窗口的情况下，每来一条数据就输出一次，太过频繁也没有必要
 * 开窗口后，只有水位线到达窗口关闭时间时才会输出
 */
```

```java
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
```

```java
/**
 * 4、迟到数据的处理
 * 1）设置watermark延迟时间
 *   逻辑时间调慢
 * 2）允许窗口处理迟到数据，设置窗口延迟关闭
 *   默认情况下水位线到达窗口结束时间时会关闭窗口并进行计算输出结果
 *   设置窗口延迟关闭后，水位线到达时会输出一个计算结果值，后续在延迟关闭时间内每达到一个延迟数据会进行一次计算并输出结果，直到延迟时间结束
 * 3）侧输出流，将最后的迟到数据输出到侧输出流
 *    最终的兜底处理
 */
```

### 六、处理函数

```java
处理函数：
1、ProcessFunction
2、KeyedProcessFunction
3、ProcessWindowFunction
4、ProcessAllWindowFunction
5、CoProcessFunction
6、KeyedCoProcessFunction
7、ProcessJoinFunction
8、BroadcastProcessFunction
9、KeyedBroadcastProcessFunction
```

```java
/**
 * 1、基本处理函数 org.apache.flink.streaming.api.functions.ProcessFunction
 * - processElement() 流中的每个元素都会调用一次，处理元素的核心逻辑
 * - onTimer() 注册的定时器触发时的处理逻辑 （注：只有按键分区流才能设置定时器操作）
 */
```

```java
/**
 * 2、按键分区处理函数 org.apache.flink.streaming.api.functions.KeyedProcessFunction
 * keyBy() 之后的处理
 * - processElement()
 * - onTimer()
 * <p>
 * 可以注册定时器:
 * 1）处理时间定时器 ctx.timerService().registerProcessingTimeTimer()
 * 2）事件时间定时器 ctx.timerService().registerEventTimeTimer()
 * <p>
 * 获取当前水位线信息：
 * ctx.timerService().currentWatermark()
 */
```

```java
/**
 * 3、全窗口处理函数 org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction
 * windowAll() 之后调用 process() 的处理
 * - process()
 * <p>
 * 全窗口 TopN 问题
 */
```

```java
/**
 * 4、窗口处理函数 org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
 * window() 之后调用 process() 的处理
 * - process()
 * <p>
 * 窗口 TopN 问题
 */
```

### 七、分流与合流

```java
/**
 * 1、分流
 * - filter
 * 通过 filter() 筛选出符合添加的数据
 * 问题：重复使用数据源，数据流的复制，不够高效
 * <p>
 * - outputTag 侧输出流
 * 通过侧输出流进行分流处理
 * OutputTag 及 ProcessFunction 处理函数
 */
```

```java
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
```

### 八、状态编程

```java
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
```

### 九、Flink Table & SQL

```java
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
```

```java
/**
 * 2、Flink Table & SQL 基础使用 {@see https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/table/overview/}
 * 1）TableEnvironment#executeSql() 用于执行 DDL/DML/DQL/SHOW/DESCRIBE/EXPLAIN/USE
 * 2）TableEnvironment#sqlQuery() 用于执行 SQL
 * 3）TableEnvironment#createTemporaryView() 用于创建临时表，创建后可以在SQL中直接使用
 * 4）TableEnvironment#from(tableName) 用于创建Table对象
 * <p>
 * Table API：
 * 1）Table#select() 查询指定的列
 * 2）Table#where() 查询条件
 * 3）Table#as() 给列取别名
 * 4）Table#groupBy() 分组
 * 5）Table#orderBy() 排序
 * 6）Table#limit() 分页
 * 7）Table#limit() 分页
 * 8）Table#join() 内连接
 * 9）Table#leftOuterJoin() 左外连接
 * ...
 */
```

```java
/**
 * 3、表转换成数据流
 * 1）StreamTableEnvironment#toDataStream(Table) - 不存在更新 -U +U 操作的可以使用
 * 2）StreamTableEnvironment#toChangelogStream(Table)
 */
```

```java
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
```

```java
/**
 * 8、标量函数（Scalar Functions）：将输入的标量值转换成一个新的标量值
 * 1）自定义一个类来继承抽象类 ScalarFunction
 * 2）要求求值方法必须名字为 eval()，eval() 方法有入参和返回值
 *
 * 注册UDF：
 * StreamTableEnvironment#createTemporarySystemFunction()
 */
```

```java
/**
 * 9、表函数（Table Functions）：将标量值转换成一个或多个新的行数据，也就是扩展成一个表
 * 1）自定义类来继承抽象类 TableFunction
 * 2）实现的也是一个名为 eval 的求值方法，eval()方法没有返回类型，内部也没有 return语句，是通过调用 collect()方法来发送想要输出的行数据的
 */
```

```java
/**
 * 10、聚合函数（Aggregate Functions）：将多行数据里的标量值转换成一个新的标量值
 * 1）继承抽象类 AggregateFunction
 */
```

```java
/**
 * 11、表聚合函数（Table Aggregate Functions）：将多行数据里的标量值转换成一个或多个新的行数据
 * 1）继承抽象类 TableAggregateFunction
 */
```

### 十、Flink CEP



