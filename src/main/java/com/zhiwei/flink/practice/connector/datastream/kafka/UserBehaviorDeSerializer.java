package com.zhiwei.flink.practice.connector.datastream.kafka;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

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
        return userBehavior.toString().getBytes();
    }

    @Override
    public TypeInformation<UserBehavior> getProducedType() {
        return TypeInformation.of(UserBehavior.class);
    }
}
