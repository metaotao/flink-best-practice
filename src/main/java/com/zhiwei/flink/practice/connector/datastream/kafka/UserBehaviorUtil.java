package com.zhiwei.flink.practice.connector.datastream.kafka;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class UserBehaviorUtil {

    public static StreamExecutionEnvironment prepareExecuteEnv() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 10000));
        env.enableCheckpointing(5000); // create a checkpoint every 5 seconds
        return env;
    }
}
