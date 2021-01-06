package com.zhiwei.flink.practice.flinkstreaming.kafka;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class UserBehaviorDeSerializer implements DeserializationSchema<UserBehavior>,
        SerializationSchema<UserBehavior> {
    @Override
    public UserBehavior deserialize(byte[] bytes){

        return UserBehavior.fromString(new String(bytes));
    }

    @Override
    public boolean isEndOfStream(UserBehavior userBehavior) {
        return false;
    }

    @Override
    public byte[] serialize(UserBehavior userBehavior) {
        return new byte[0];
    }

    @Override
    public TypeInformation<UserBehavior> getProducedType() {
        return null;
    }
}
