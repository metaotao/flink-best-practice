package com.zhiwei.flink.practice.flinkstreaming.kafka;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;

import java.time.Duration;
import java.util.Properties;

public class DataStreamKafka {
    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties inputProperties = new Properties();
        inputProperties.setProperty("bootstrap.servers", "localhost:9092");
        inputProperties.setProperty("group.id", "user_behavior_input");
        DataStream<UserBehavior> stream  = environment
                .addSource(new FlinkKafkaConsumer<>("user_behavior_input",
                new UserBehaviorDeSerializer(), inputProperties))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner((SerializableTimestampAssigner<UserBehavior>) (o, l) -> o.getTimestamp())
                ).filter((FilterFunction<UserBehavior>) userBehavior -> {
                    // 过滤出只有点击的数据
                    return userBehavior.behavior.equals("pv");
                });

        Properties outputProperties = new Properties();
        outputProperties.setProperty("bootstrap.servers", "localhost:9092");
        outputProperties.setProperty("group.id", "user_behavior_output");
        stream.addSink(new FlinkKafkaProducer<>("user_behavior_output",
                (KafkaSerializationSchema<UserBehavior>) new UserBehaviorDeSerializer(),
                outputProperties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
        environment.execute();

    }
}
