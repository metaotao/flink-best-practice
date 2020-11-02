package com.zhiwei.flink.practice.tablesql.source;

import com.zhiwei.flink.practice.tablesql.datatypes.TaxiRide;
import com.zhiwei.flink.practice.tablesql.descripors.TaxiRides;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
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

        return streamExecutionEnvironment
                .addSource(this.taxiRideSource)
                .map(new TaxiRideToRow()).returns(getReturnType());
    }

    @Override
    public TypeInformation<Row> getReturnType() {

        TypeInformation<?>[] types = new TypeInformation[] {
                Types.LONG,
                Types.LONG,
                Types.LONG,
                Types.BOOLEAN,
                Types.FLOAT,
                Types.FLOAT,
                Types.FLOAT,
                Types.FLOAT,
                Types.SHORT
        };

        String[] names = new String[]{
                "rideId",
                "taxiId",
                "driverId",
                "isStart",
                "startLon",
                "startLat",
                "endLon",
                "endLat",
                "passengerCnt"
        };

        return new RowTypeInfo(types, names);
    }

    @Override
    public TableSchema getTableSchema() {
        TypeInformation<?>[] types = new TypeInformation[] {
                Types.LONG,
                Types.LONG,
                Types.LONG,
                Types.BOOLEAN,
                Types.FLOAT,
                Types.FLOAT,
                Types.FLOAT,
                Types.FLOAT,
                Types.SHORT,
                Types.SQL_TIMESTAMP
        };

        String[] names = new String[]{
                "rideId",
                "taxiId",
                "driverId",
                "isStart",
                "startLon",
                "startLat",
                "endLon",
                "endLat",
                "passengerCnt",
                "eventTime"
        };

        return new TableSchema(names, types);
    }

    /**
     * Converts TaxiRide records into table Rows.
     */
    public static class TaxiRideToRow implements MapFunction<TaxiRide, Row> {

        @Override
        public Row map(TaxiRide ride) throws Exception {

            return Row.of(
                    ride.rideId,
                    ride.taxiId,
                    ride.driverId,
                    ride.isStart,
                    ride.startLon,
                    ride.startLat,
                    ride.endLon,
                    ride.endLat,
                    ride.passengerCnt);
        }
    }
}
