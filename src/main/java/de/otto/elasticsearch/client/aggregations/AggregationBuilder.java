package de.otto.elasticsearch.client.aggregations;

import com.google.gson.JsonObject;
import de.otto.elasticsearch.client.response.Aggregation;

public interface AggregationBuilder {
    JsonObject build();
    Aggregation parseResponse(JsonObject jsonObject);
}
