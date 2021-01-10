package com.zhiwei.flink.practice.datastreaming.example;

import com.zhiwei.flink.practice.datastreaming.example.datatypes.TaxiRide;
import com.zhiwei.flink.practice.datastreaming.example.sources.TaxiRideGenerator;
import com.zhiwei.flink.practice.datastreaming.example.utils.ExerciseBase;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RideCleansingExercise  extends ExerciseBase {

    public static void main(String[] args) throws Exception {
        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(ExerciseBase.parallelism);

        DataStream<TaxiRide> rides = env.addSource(rideSourceOrTest(new TaxiRideGenerator()));

        DataStream<TaxiRide> filteredRides = rides
                // filter out rides that do not start or stop in NYC
                .filter(new NYCFilter());

        // print the filtered stream
        printOrTest(filteredRides);

        // run the cleansing pipeline
        env.execute("Taxi Ride Cleansing");
    }

    private static class NYCFilter implements FilterFunction<TaxiRide> {

        @Override
        public boolean filter(TaxiRide taxiRide) {
            return taxiRide.rideId > 10;

        }
    }
}
