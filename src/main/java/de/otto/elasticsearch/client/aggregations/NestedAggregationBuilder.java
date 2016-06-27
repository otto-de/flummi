package de.otto.elasticsearch.client.aggregations;

import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import de.otto.elasticsearch.client.response.AggregationResult;

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

        if (path==null || path.isEmpty()) {
            throw new RuntimeException("missing property 'path'");
        }
        if (subAggregations == null) {
            throw new RuntimeException("property 'nestedAggregations' is missing");
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
        return AggregationResultParser.parseSubAggregations(jsonObject, subAggregations);
    }

}
