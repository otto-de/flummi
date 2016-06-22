package de.otto.elasticsearch.client.aggregations;

import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import de.otto.elasticsearch.client.response.AggregationResult;
import de.otto.elasticsearch.client.util.StringUtils;

import java.util.HashMap;
import java.util.Map;

public class NestedAggregationBuilder extends AggregationBuilder<NestedAggregationBuilder> {
    private String path;

    public NestedAggregationBuilder(String name) {
        super(name);
    }

    public NestedAggregationBuilder path(String path) {
        this.path = path;
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
        JsonObject aggsJson = new JsonObject();

        subAggregations.stream().forEach(a -> aggsJson.add(a.getName(), a.build()));
        jsonObject.add("aggregations", aggsJson);
        return jsonObject;
    }

    @Override
    public AggregationResult parseResponse(JsonObject jsonObject) {
        Map<String, AggregationResult> aggregations = new HashMap<>();

        if (subAggregations != null) {
            subAggregations.stream().forEach(t ->
                    aggregations.put(t.getName(), t.parseResponse(jsonObject.get(t.getName()).getAsJsonObject())));
        }

        return new AggregationResult(aggregations);
    }

}
