package com.zhiwei.flink.practice.tablesql.example;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;
import java.time.Instant;
import java.util.Random;

import static org.apache.flink.table.api.Expressions.$;

public class Sort {

    public static final int OUT_OF_ORDERNESS = 1000;

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        env.setParallelism(1);

        DataStream<Event> eventDataStream = env.addSource(new OutOfOrderSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner((SerializableTimestampAssigner<Event>) (event, l) -> event.eventTime));

        Table events = tEnv.fromDataStream(eventDataStream, $("eventTime.rowtime"));

        tEnv.createTemporaryView("events", events);
        Table sorted = tEnv.sqlQuery("SELECT eventTime FROM events ORDER BY eventTime ASC");
        DataStream<Row> sortedEventStream = tEnv.toAppendStream(sorted, Row.class);

        sortedEventStream.print();

        env.execute();
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
