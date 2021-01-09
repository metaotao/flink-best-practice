package com.zhiwei.flink.practice.flinkstreaming.kafka;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;

import javax.annotation.Nullable;
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

                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                    private long currentTimestamp = Long.MIN_VALUE;

                    @Override
                    public long extractTimestamp(UserBehavior o, long l) {
                        this.currentTimestamp = o.getTimestamp();
                        return o.getTimestamp();
                    }
                    @Nullable
                    public Watermark getCurrentWatermark() {
                        return new Watermark(
                                currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp - 1);
                    }
                }));

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
