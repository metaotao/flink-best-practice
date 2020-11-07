package com.zhiwei.flink.practice.flinkstreaming;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.source.FromElementsFunction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DataStreamTraining {
    private static List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

    public static void main(String[] args) throws Exception {
        dataStream();
    }



    public static void dataStream() throws Exception {
        StreamExecutionEnvironment e = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Integer> source = e.addSource(
                new FromElementsFunction<>(Types.INT.createSerializer(e.getConfig()), data), Types.INT);
        DataStream<Integer> ds = source.map(v -> v * 2).keyBy(value -> 1).sum(0);
        ds.addSink(new PrintSinkFunction<>());
        System.out.println(e.getExecutionPlan());
        e.execute();
    }
}
