package com.zhiwei.flink.practice.datastreaming.example.sources;

import com.zhiwei.flink.practice.datastreaming.example.datatypes.TaxiFare;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

public class TaxiFareGenerator implements SourceFunction<TaxiFare> {

    private volatile boolean running = true;

    @Override
    public void run(SourceContext<TaxiFare> sourceContext) throws Exception {
        long id = 1;

        while (running) {
            TaxiFare fare = new TaxiFare(id);
            id += 1;

            sourceContext.collectWithTimestamp(fare, fare.getEventTime());
            sourceContext.emitWatermark(new Watermark(fare.getEventTime()));

            // match our event production rate to that of the TaxiRideGenerator
            Thread.sleep(TaxiRideGenerator.SLEEP_MILLIS_PER_EVENT);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
