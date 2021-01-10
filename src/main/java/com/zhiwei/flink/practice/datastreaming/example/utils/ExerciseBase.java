package com.zhiwei.flink.practice.datastreaming.example.utils;

import com.zhiwei.flink.practice.datastreaming.example.datatypes.TaxiFare;
import com.zhiwei.flink.practice.datastreaming.example.datatypes.TaxiRide;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class ExerciseBase {

    public static SourceFunction<TaxiRide> rides = null;

    public static SourceFunction<TaxiFare> fares = null;
    public static SourceFunction<String> strings = null;
    public static SinkFunction out = null;
    public static int parallelism = 4;

    /**
     * Retrieves a test source during unit tests and the given one during normal execution.
     */
    public static SourceFunction<TaxiRide> rideSourceOrTest(SourceFunction<TaxiRide> source) {
        if (rides == null) {
            return source;
        }
        return rides;
    }
    /**
     * Retrieves a test source during unit tests and the given one during normal execution.
     */
    public static SourceFunction<TaxiFare> fareSourceOrTest(SourceFunction<TaxiFare> source) {
        if (fares == null) {
            return source;
        }
        return fares;
    }

    public static void printOrTest(DataStream<?> ds) {

        if (out == null) {
            ds.print();
        } else{
            ds.addSink(out);
        }
    }

}
