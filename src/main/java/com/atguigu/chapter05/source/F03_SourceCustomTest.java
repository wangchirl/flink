package com.atguigu.chapter05.source;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial
 * <p>
 * Created by  wushengran
 */

import com.atguigu.chapter05.common.ClickSource;
import com.atguigu.chapter05.common.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class F03_SourceCustomTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //有了自定义的source function，调用addSource方法
        DataStreamSource<Event> stream = env.addSource(new ClickSource());

        stream.print("SourceCustom");

        env.execute();
    }
}

