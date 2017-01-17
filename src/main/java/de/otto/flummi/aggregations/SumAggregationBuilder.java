package de.otto.flummi.aggregations;

import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import de.otto.flummi.response.SumAggregationResult;

public class SumAggregationBuilder implements AggregationBuilder {

    private final String aggregationName;
    private final String aggregationField;

    public SumAggregationBuilder(String aggregationName, String aggregationField) {
        this.aggregationName = aggregationName;
        this.aggregationField = aggregationField;
    }

    @Override
    public String getName() {
        return aggregationName;
    }

    @Override
    public JsonObject build() {
        JsonObject jsonObject = new JsonObject();
        JsonObject fields = new JsonObject();
        fields.add("field", new JsonPrimitive(aggregationField));
        jsonObject.add("sum", fields);

        return jsonObject;
    }

    @Override
    public SumAggregationResult parseResponse(JsonObject jsonObject) {
        return new SumAggregationResult(jsonObject.get("value").getAsNumber());
    }
}
