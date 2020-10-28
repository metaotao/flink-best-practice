package com.zhiwei.flink.practice.tablesql.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.DefinedRowtimeAttributes;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

import java.util.List;

public class TaxiRideTableSource  implements StreamTableSource<Row>, DefinedRowtimeAttributes {

    private final TaxiRideTableSource taxiRideSource;

    public TaxiRideTableSource(String dataFilePath) {
        this.taxiRideSource = new TaxiRideTableSource(dataFilePath);
    }

    /**
     * Serves the taxi ride rows from the specified and ordered gzipped input file.
     * Rows are served exactly in order of their time stamps
     * in a serving speed which is proportional to the specified serving speed factor.
     *
     * @param dataFilePath The gzipped input file from which the taxi ride rows are read.
     * @param servingSpeedFactor The serving speed factor by which the logical serving time is adjusted.
     */
    public TaxiRideTableSource(String dataFilePath, int servingSpeedFactor) {
        this.taxiRideSource = new TaxiRideTableSource(dataFilePath, 0, servingSpeedFactor);
    }

    /**
     * Serves the taxi ride rows from the specified and ordered gzipped input file.
     * Rows are served out-of time stamp order with specified maximum random delay
     * in a serving speed which is proportional to the specified serving speed factor.
     *
     * @param dataFilePath The gzipped input file from which the taxi ride rows are read.
     * @param maxEventDelaySecs The max time in seconds by which rows are delayed.
     * @param servingSpeedFactor The serving speed factor by which the logical serving time is adjusted.
     */
    public TaxiRideTableSource(String dataFilePath, int maxEventDelaySecs, int servingSpeedFactor) {
        this.taxiRideSource = new TaxiRideTableSource(dataFilePath, maxEventDelaySecs, servingSpeedFactor);
    }

    @Override
    public List<RowtimeAttributeDescriptor> getRowtimeAttributeDescriptors() {
        return null;
    }

    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment streamExecutionEnvironment) {
        return null;
    }

    @Override
    public TableSchema getTableSchema() {
        return null;
    }
}
