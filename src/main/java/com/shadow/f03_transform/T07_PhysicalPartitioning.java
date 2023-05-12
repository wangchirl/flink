package com.shadow.f03_transform;

import com.shadow.f00_pojo.ClickEvent;
import com.shadow.f02_source.ClickParallelSource;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

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
public class T07_PhysicalPartitioning {
    public static void main(String[] args) throws Exception {
        // 1、获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2、添加数据源
        DataStreamSource<ClickEvent> clickDS = env.addSource(new ClickParallelSource());


        // 3、数据转换
        // 4、数据输出
        // 1.shuffle
        // clickDS.shuffle().print("shuffle").setParallelism(4);
        // 2.rebalance
        // clickDS.rebalance().print("rebalance").setParallelism(4);
        // 3.rescale
        /*env.addSource(new RichParallelSourceFunction<Integer>() {
            @Override
            public void run(SourceContext<Integer> ctx) throws Exception {
                for (int i = 0; i < 8; i++) { // 并行度2 -> 奇偶数分开发送到2个分区
                    if ((i + 1) % 2 == getRuntimeContext().getIndexOfThisSubtask()) {
                        ctx.collect(i);
                    }
                }
            }

            @Override
            public void cancel() {

            }
        }).setParallelism(2) // 1和2
                .rescale() // 重缩放分区：1 -> 1和2  2-> 3和4
                .print().setParallelism(4);*/
        // 4.broadcast
        // clickDS.broadcast().print("broadcast").setParallelism(4);
        // 5.global
        // clickDS.setParallelism(2).global().print("global").setParallelism(4);
        // 6.partitionCustom
        env.fromCollection(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8))
                .partitionCustom(new Partitioner<Integer>() {
                                     @Override
                                     public int partition(Integer key, int numPartitions) {
                                         return key % 2;
                                     }
                                 }, new KeySelector<Integer, Integer>() {
                                     @Override
                                     public Integer getKey(Integer value) throws Exception {
                                         return value;
                                     }
                                 }
                ).print("custom").setParallelism(2);

        // 5、执行
        env.execute();
    }
}
