package com.zhiwei.flink.practice.flinkstreaming.example;

import com.zhiwei.flink.practice.flinkstreaming.example.utils.ExerciseBase;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RideCleansingExercise  extends ExerciseBase {

    public static void main(String[] args)  throws Exception {
        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(ExerciseBase.parallelism);

    }
}
