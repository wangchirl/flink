package com.shadow.f04_sink;

import com.shadow.f00_pojo.ClickEvent;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

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
public class T05_ElasticsearchSink {

    public static void main(String[] args) throws Exception {
        // 1、获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2、添加数据源
        DataStreamSource<ClickEvent> clickDS = env.fromElements(
                new ClickEvent("shadow", "./index", 1000L),
                new ClickEvent("oracle", "./index", 2000L),
                new ClickEvent("shadow", "./cart", 3000L),
                new ClickEvent("oracle", "./home", 5000L)
        );

        // 3、数据转换
        // 4、数据输出
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("localhost", 9200, "http"));
        ElasticsearchSinkFunction<ClickEvent> elasticsearchSinkFunction = new ElasticsearchSinkFunction<ClickEvent>() {
            @Override
            public void process(ClickEvent clickEvent, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                HashMap<String, String> data = new HashMap<>();
                data.put(clickEvent.user, clickEvent.url);

                IndexRequest request = Requests.indexRequest()
                        .index("clicks")
                        .type("type")    // Es 6 必须定义 type
                        .source(data);

                requestIndexer.add(request);
            }
        };
        clickDS.addSink(new ElasticsearchSink.Builder<ClickEvent>(httpHosts, elasticsearchSinkFunction).build());

        // 5、执行
        env.execute();
    }
}
