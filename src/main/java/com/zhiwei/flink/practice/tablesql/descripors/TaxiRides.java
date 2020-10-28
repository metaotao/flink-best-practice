package com.zhiwei.flink.practice.tablesql.descripors;

import org.apache.flink.calcite.shaded.com.google.common.base.Preconditions;
import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.descriptors.DescriptorProperties;

import java.util.Map;

import static com.zhiwei.flink.practice.tablesql.descripors.TaxiRidesValidator.*;

public class TaxiRides extends ConnectorDescriptor {
    public TaxiRides() {
        super(CONNECTOR_TYPE_VALUE_TAXI_RIDES, 1, false);
    }

    private String path;
    private Integer maxEventDelaySecs;
    private Integer servingSpeedFactor;

    public TaxiRides path(String path) {
        this.path = Preconditions.checkNotNull(path);
        return this;
    }

    public TaxiRides maxEventDelaySecs(int maxEventDelaySecs) {
        this.maxEventDelaySecs = maxEventDelaySecs;
        return this;
    }

    public TaxiRides servingSpeedFactor(int servingSpeedFactor) {
        this.servingSpeedFactor = servingSpeedFactor;
        return this;
    }

    @Override
    protected Map<String, String> toConnectorProperties() {
        DescriptorProperties properties = new DescriptorProperties();
        if (this.path != null) {
            properties.putString(CONNECTOR_PATH, this.path);
        }
        if (this.maxEventDelaySecs != null) {
            properties.putInt(CONNECTOR_MAX_EVENT_DELAY_SECS, this.maxEventDelaySecs);
        }
        if (this.servingSpeedFactor != null) {
            properties.putInt(CONNECTOR_SERVING_SPEED_FACTOR, this.servingSpeedFactor);
        }
        return properties.asMap();
    }
}
