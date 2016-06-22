package de.otto.elasticsearch.client.aggregations;

import com.google.gson.JsonObject;
import de.otto.elasticsearch.client.query.QueryBuilder;
import de.otto.elasticsearch.client.response.AggregationResult;

import java.util.HashMap;
import java.util.Map;

import static de.otto.elasticsearch.client.request.GsonHelper.object;

public class FilterAggregationBuilder extends AggregationBuilder<FilterAggregationBuilder> {

    private QueryBuilder filter;

    public FilterAggregationBuilder(String name) {
        super(name);
    }

    public FilterAggregationBuilder withFilter(QueryBuilder filter) {
        this.filter = filter;
        return this;
    }


    @Override
    public JsonObject build() {
        if (subAggregations == null) {
            throw new RuntimeException("No subaggregations defined");
        }
        JsonObject aggsJson = new JsonObject();
        subAggregations.stream().forEach(a -> aggsJson.add(a.getName(), a.build()));
        return object("filter", filter.build(),
                "aggregations", aggsJson);
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
