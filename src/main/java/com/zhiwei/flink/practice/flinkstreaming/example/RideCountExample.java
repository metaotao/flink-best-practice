package com.zhiwei.flink.practice.flinkstreaming.example;

import com.zhiwei.flink.practice.flinkstreaming.example.datatypes.TaxiRide;
import com.zhiwei.flink.practice.flinkstreaming.example.sources.TaxiRideGenerator;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RideCountExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<TaxiRide> rides = env.addSource(new TaxiRideGenerator());

        DataStream<Tuple2<Long, Long>> mapRides = rides.map(
                (MapFunction<TaxiRide, Tuple2<Long, Long>>)
                        taxiRide -> Tuple2.of(taxiRide.driverId, 1L));

        KeyedStream<Tuple2<Long, Long>, Long> keyedRide = mapRides.keyBy(t -> t.f0);

        DataStream<Tuple2<Long, Long>> countRide = keyedRide.sum(1);

        countRide.print();

        env.execute("Ride Count");


    }
}
