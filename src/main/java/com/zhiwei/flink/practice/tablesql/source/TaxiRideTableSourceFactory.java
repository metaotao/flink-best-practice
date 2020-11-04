package com.zhiwei.flink.practice.tablesql.source;

import com.zhiwei.flink.practice.tablesql.descripors.TaxiRidesValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.StreamTableDescriptorValidator;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.zhiwei.flink.practice.tablesql.descripors.TaxiRidesValidator.*;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;

public class TaxiRideTableSourceFactory implements StreamTableSourceFactory<Row> {
    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> context = new HashMap<>();
        context.put(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE_TAXI_RIDES); // taxi-rides
        context.put(CONNECTOR_PROPERTY_VERSION, "1"); // backwards compatibility
        return context;
    }

    @Override
    public List<String> supportedProperties() {
        List<String> properties = new ArrayList<>();
        properties.add(StreamTableDescriptorValidator.UPDATE_MODE);
        properties.add(CONNECTOR_PATH);
        properties.add(CONNECTOR_MAX_EVENT_DELAY_SECS);
        properties.add(CONNECTOR_SERVING_SPEED_FACTOR);
        return properties;
    }

    @Override
    public TaxiRideTableSource createStreamTableSource(Map<String, String> properties) {
        DescriptorProperties params = getValidatedProperties(properties);
        return new TaxiRideTableSource(
                params.getString(CONNECTOR_PATH),
                params.getOptionalInt(CONNECTOR_MAX_EVENT_DELAY_SECS).orElse(0),
                params.getOptionalInt(CONNECTOR_SERVING_SPEED_FACTOR).orElse(1)
        );
    }

    private DescriptorProperties getValidatedProperties(Map<String, String> properties) {
        final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
        descriptorProperties.putProperties(properties);

        new StreamTableDescriptorValidator(true, false, false).validate(descriptorProperties);
        new TaxiRidesValidator().validate(descriptorProperties);

        return descriptorProperties;
    }
}
