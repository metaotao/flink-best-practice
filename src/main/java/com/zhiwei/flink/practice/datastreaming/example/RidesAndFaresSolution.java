package com.zhiwei.flink.practice.datastreaming.example;

import com.zhiwei.flink.practice.datastreaming.example.datatypes.TaxiFare;
import com.zhiwei.flink.practice.datastreaming.example.datatypes.TaxiRide;
import com.zhiwei.flink.practice.datastreaming.example.sources.TaxiFareGenerator;
import com.zhiwei.flink.practice.datastreaming.example.sources.TaxiRideGenerator;
import com.zhiwei.flink.practice.datastreaming.example.utils.ExerciseBase;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

public class RidesAndFaresSolution extends ExerciseBase {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration() ;

        conf.setString("state.backend", "filesystem");
        conf.setString("state.savepoints.dir", "file:///tmp/savepoints");
        conf.setString("state.checkpoints.dir", "file:///tmp/checkpoints");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(ExerciseBase.parallelism);

        env.enableCheckpointing(1000L);

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);


        DataStream<TaxiRide> taxiRideDataStream = env.addSource(new TaxiRideGenerator())
                .filter((TaxiRide ride) -> ride.isStart)
                .keyBy((TaxiRide ride) -> ride.rideId);

        DataStream<TaxiFare> fares = env
                .addSource(fareSourceOrTest(new TaxiFareGenerator()))
                .keyBy((TaxiFare fare) -> fare.rideId);


        // Set a UID on the stateful flatmap operator so we can read its state using the State Processor API.
        DataStream<Tuple2<TaxiRide, TaxiFare>> enrichedRides = taxiRideDataStream
                .connect(fares)
                .flatMap(new EnrichmentFunction())
                .uid("enrichment");

        printOrTest(enrichedRides);

        env.execute("Join Rides with Fares (java RichCoFlatMap)");

    }

    public static class EnrichmentFunction extends RichCoFlatMapFunction<TaxiRide, TaxiFare, Tuple2<TaxiRide, TaxiFare>> {

        private ValueState<TaxiRide> rideValueState;

        private ValueState<TaxiFare> fareValueState;

        @Override
        public void open(Configuration configuration) {
            rideValueState = getRuntimeContext().getState(new ValueStateDescriptor<>("saved ride", TaxiRide.class));
            fareValueState = getRuntimeContext().getState(new ValueStateDescriptor<>("saved fare", TaxiFare.class));        }

        @Override
        public void flatMap1(TaxiRide ride, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
            TaxiFare fare = fareValueState.value();
            if (fare != null) {
                fareValueState.clear();
                out.collect(Tuple2.of(ride, fare));
            } else {
                rideValueState.update(ride);
            }
        }

        @Override
        public void flatMap2(TaxiFare fare, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
            TaxiRide ride = rideValueState.value();
            if (ride != null) {
                rideValueState.clear();
                out.collect(Tuple2.of(ride, fare));
            } else {
                fareValueState.update(fare);
            }
        }
    }
}
