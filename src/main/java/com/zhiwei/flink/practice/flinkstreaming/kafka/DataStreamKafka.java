package com.zhiwei.flink.practice.flinkstreaming.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;

public class DataStreamKafka {
    public static void main(String[] args) {


        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "user_behavior");
        environment.addSource(new FlinkKafkaConsumer<>("user_behavior", new SimpleStringSchema(), properties));

        //        environment.addSource(new FlinkKafkaConsumer<>())
    }
}
