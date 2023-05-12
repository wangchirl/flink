package com.shadow.f00_pojo;

import java.sql.Timestamp;

/**
 * flink pojo 要求：
 * 1、类是公有（public）的
 * 2、有一个无参的构造方法
 * 3、所有属性都是公有（public）的
 * 4、所有属性的类型都是可以序列化的
 */
public class ClickEvent {

    public String user;
    public String url;
    public long timestamp;

    public ClickEvent() {
    }

    public ClickEvent(String user, String url, long timestamp) {
        this.user = user;
        this.url = url;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Event{" +
                "user='" + user + '\'' +
                ", url='" + url + '\'' +
                ", timestamp=" + new Timestamp(timestamp) +
                '}';
    }
}
