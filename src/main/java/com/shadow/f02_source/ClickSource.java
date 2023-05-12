package com.shadow.f02_source;

import com.shadow.f00_pojo.ClickEvent;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class ClickSource implements SourceFunction<ClickEvent> {

        private boolean running = true;

        @Override
        public void run(SourceContext<ClickEvent> ctx) throws Exception {
            Random random = new Random();
            String[] users = {"shadow", "oracle", "apache"};
            String[] urls = {"./home", "./prod", "./index", "./login", "./cart"};
            while (running) {
                ctx.collect(new ClickEvent(users[random.nextInt(users.length)], urls[random.nextInt(urls.length)], System.currentTimeMillis()));
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            this.running = false;
        }
    }