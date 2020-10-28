package com.zhiwei.flink.practice.tablesql.source;

import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.types.Row;

import java.util.List;
import java.util.Map;

public class TaxiRideTableSourceFactory implements StreamTableSourceFactory<Row> {
    @Override
    public Map<String, String> requiredContext() {
        return null;
    }

    @Override
    public List<String> supportedProperties() {
        return null;
    }
}
