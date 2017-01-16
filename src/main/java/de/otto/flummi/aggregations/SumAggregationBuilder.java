package de.otto.flummi.aggregations;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import de.otto.flummi.response.SumAggregationResult;

import static de.otto.flummi.request.GsonHelper.object;

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
        JsonObject customField = new JsonObject();
        fields.add("field", new JsonPrimitive(aggregationField));
        jsonObject.add("sum", fields);
        customField.add(aggregationName, jsonObject);

        return object("aggs", customField);
    }

    @Override
    public SumAggregationResult parseResponse(JsonObject jsonObject) {
        JsonElement jsonElement = jsonObject.get(aggregationName);
        return new SumAggregationResult(jsonElement.getAsJsonObject().get("value").getAsNumber());
    }
}
