package com.zhiwei.flink.practice.flinkstreaming;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class DataStreamKafka {
    public static void main(String[] args) {


        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

//        environment.addSource(new FlinkKafkaConsumer<>())
    }
}
