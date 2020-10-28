package com.zhiwei.flink.practice.tablesql.descripors;

import org.apache.flink.table.descriptors.ConnectorDescriptorValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;

public class TaxiRidesValidator extends ConnectorDescriptorValidator {
    public static final String CONNECTOR_TYPE_VALUE_TAXI_RIDES = "taxi-rides";
    public static final String CONNECTOR_PATH = "connector.path";
    public static final String CONNECTOR_MAX_EVENT_DELAY_SECS = "connector.max-event-delay-secs";
    public static final String CONNECTOR_SERVING_SPEED_FACTOR = "connector.serving-speed-factor";

    @Override
    public void validate(DescriptorProperties properties) {
        super.validate(properties);
        properties.validateValue(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE_TAXI_RIDES, false);
        properties.validateString(CONNECTOR_PATH, false);
        properties.validateInt(CONNECTOR_MAX_EVENT_DELAY_SECS, true, 0);
        properties.validateInt(CONNECTOR_SERVING_SPEED_FACTOR, true, 1);
    }
}
