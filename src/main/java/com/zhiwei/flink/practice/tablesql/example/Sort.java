package com.zhiwei.flink.practice.tablesql.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Instant;
import java.util.Random;

public class Sort {

    public static final int OUT_OF_ORDERNESS = 1000;

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        env.setParallelism(1);


    }


    public static class Event {

        public Long eventTime;
        Event() {
            this.eventTime = Instant.now().toEpochMilli() + (new Random().nextInt(OUT_OF_ORDERNESS));
        }

    }

    public static class OutOfOrderSource implements SourceFunction<Event> {

        public volatile boolean running = true;
        @Override
        public void run(SourceContext ctx) throws Exception {
            while (running) {
                ctx.collect(new Event());
                Thread.sleep(1);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

}
