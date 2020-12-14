package com.zhiwei.flink.practice.flinkstreaming.example;

import com.zhiwei.flink.practice.flinkstreaming.example.datatypes.TaxiFare;
import com.zhiwei.flink.practice.flinkstreaming.example.datatypes.TaxiRide;
import com.zhiwei.flink.practice.flinkstreaming.example.sources.TaxiFareGenerator;
import com.zhiwei.flink.practice.flinkstreaming.example.sources.TaxiRideGenerator;
import com.zhiwei.flink.practice.flinkstreaming.example.utils.ExerciseBase;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

public class RidesAndFaresExercise extends ExerciseBase {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(ExerciseBase.parallelism);
        DataStream<TaxiRide> rides = env
                .addSource(rideSourceOrTest(new TaxiRideGenerator()))
                .filter((TaxiRide ride) -> ride.isStart)
                .keyBy((TaxiRide ride) -> ride.rideId);

        DataStream<TaxiFare> fares = env
                .addSource(fareSourceOrTest(new TaxiFareGenerator()))
                .keyBy((TaxiFare fare) -> fare.rideId);

        DataStream<Tuple2<TaxiRide, TaxiFare>> enrichedRides = rides
                .connect(fares)
                .flatMap(new EnrichmentFunction()).uid("enrichment");

        printOrTest(enrichedRides);

        env.execute("Join Rides with Fares (java RichCoFlatMap)");

    }

    public static class EnrichmentFunction extends RichCoFlatMapFunction<TaxiRide, TaxiFare, Tuple2<TaxiRide, TaxiFare>> {

        @Override
        public void flatMap1(TaxiRide value, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {

        }

        @Override
        public void flatMap2(TaxiFare value, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {

        }
    }

}
