package com.zhiwei.flink.practice.datastreaming.example.sources;

import com.zhiwei.flink.practice.datastreaming.example.datatypes.TaxiRide;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;

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

        while (running) {
            // generate a batch of START events
            List<TaxiRide> startEvents = new ArrayList<TaxiRide>(BATCH_SIZE);
            for (int i = 1; i <= BATCH_SIZE; i++) {
                TaxiRide ride = new TaxiRide(id + i, true);
                startEvents.add(ride);
                // the start times may be in order, but let's not assume that
                maxStartTime = Math.max(maxStartTime, ride.startTime.toEpochMilli());
            }

            // enqueue the corresponding END events
            for (int i = 1; i <= BATCH_SIZE; i++) {
                endEventQ.add(new TaxiRide(id + i, false));
            }

            // release the END events coming before the end of this new batch
            // (this allows a few END events to precede their matching START event)
            while (endEventQ.peek().getEventTime() <= maxStartTime) {
                TaxiRide ride = endEventQ.poll();
                ctx.collectWithTimestamp(ride, ride.getEventTime());
            }

            // then emit the new START events (out-of-order)
            java.util.Collections.shuffle(startEvents, new Random(id));
            startEvents.iterator().forEachRemaining(r -> ctx.collectWithTimestamp(r, r.getEventTime()));

            // produce a Watermark
            ctx.emitWatermark(new Watermark(maxStartTime));

            // prepare for the next batch
            id += BATCH_SIZE;

            // don't go too fast
            Thread.sleep(BATCH_SIZE * SLEEP_MILLIS_PER_EVENT);
        }

    }

    @Override
    public void cancel() {
        running = false;
    }
}
