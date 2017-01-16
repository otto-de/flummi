package de.otto.flummi.aggregations;

import com.google.gson.JsonObject;
import de.otto.flummi.response.AggregationResult;

public interface AggregationBuilder {

    String getName();

    JsonObject build();

    AggregationResult parseResponse(JsonObject jsonObject);
}
