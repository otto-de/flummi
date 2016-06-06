package de.otto.elasticsearch.client.aggregations;

import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import de.otto.elasticsearch.client.response.Aggregation;
import de.otto.elasticsearch.client.util.StringUtils;

import java.util.HashMap;
import java.util.Map;

public class NestedAggregationBuilder implements AggregationBuilder {
    private String path;
    private Map<String, AggregationBuilder> subAggregations;

    public NestedAggregationBuilder path(String path) {
        this.path = path;
        return this;
    }

    public NestedAggregationBuilder subAggregation(String name, AggregationBuilder subAggregation) {
        if (subAggregations == null) {
            subAggregations = new HashMap<>();
        }
        subAggregations.put(name, subAggregation);

        return this;
    }

    @Override
    public JsonObject build() {

        if (StringUtils.isEmpty(path)) {
            throw new RuntimeException("missing property 'path'");
        }
        if (subAggregations == null) {
            throw new RuntimeException("property 'termsAggregation' is missing");
        }
        JsonObject jsonObject = new JsonObject();
        JsonObject nestedObject = new JsonObject();
        jsonObject.add("nested", nestedObject);
        nestedObject.add("path", new JsonPrimitive(path));
        JsonObject subAggregations = new JsonObject();

        this.subAggregations.entrySet().stream().forEach(a ->
                subAggregations.add(a.getKey(), a.getValue().build()));
        jsonObject.add("aggregations", subAggregations);
        return jsonObject;
    }

    @Override
    public Aggregation parseResponse(JsonObject jsonObject) {
        Map<String, Aggregation> aggregations = new HashMap<>();
        if (subAggregations != null) {
            subAggregations.entrySet().stream().forEach(t ->
                    aggregations.put(t.getKey(), t.getValue().parseResponse(jsonObject.get(t.getKey()).getAsJsonObject())));
        }

        return new Aggregation(aggregations);
    }

}
