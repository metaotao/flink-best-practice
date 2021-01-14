package com.zhiwei.flink.practice.connector.datastream.jdbc;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class MyJdbcSink extends RichSinkFunction<SensorReading> {

}
