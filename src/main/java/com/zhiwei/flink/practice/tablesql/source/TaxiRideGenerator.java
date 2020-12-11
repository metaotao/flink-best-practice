package com.zhiwei.flink.practice.tablesql.source;

import com.zhiwei.flink.practice.tablesql.datatypes.TaxiRide;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.PriorityQueue;

public class TaxiRideGenerator implements SourceFunction<TaxiRide> {
    public static final int SLEEP_MILLIS_PER_EVENT = 10;
    private static final int BATCH_SIZE = 5;
    private volatile boolean running = true;
    @Override
    public void run(SourceContext<TaxiRide> ctx) throws Exception {

        // 定义优先级队列
        PriorityQueue<TaxiRide> endEventQ = new PriorityQueue<>(100);
        long id = 0;
        long maxStartTime = 0;

    }

    @Override
    public void cancel() {
        running = false;
    }
}
