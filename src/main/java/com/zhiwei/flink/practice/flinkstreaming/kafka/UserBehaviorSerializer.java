package com.zhiwei.flink.practice.flinkstreaming.kafka;

import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class UserBehaviorSerializer implements KafkaSerializationSchema<UserBehavior> {

    private String topic;

    UserBehaviorSerializer(String topic) {
        super();
        this.topic = topic;
    }
    @Override
    public ProducerRecord<byte[], byte[]> serialize(UserBehavior element, @Nullable Long timestamp) {

        return new ProducerRecord<>(topic, element.toString().getBytes());
    }
}
