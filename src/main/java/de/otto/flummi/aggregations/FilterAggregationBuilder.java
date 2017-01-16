package de.otto.flummi.aggregations;

import com.google.gson.JsonObject;
import de.otto.flummi.query.QueryBuilder;
import de.otto.flummi.response.AggregationResult;

import static de.otto.flummi.request.GsonHelper.object;

public class FilterAggregationBuilder extends SubAggregationBuilder<FilterAggregationBuilder> {

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
            throw new RuntimeException("No subAggregations defined");
        }
        JsonObject aggsJson = new JsonObject();
        subAggregations.stream().forEach(a -> aggsJson.add(a.getName(), a.build()));
        return object("filter", filter.build(),
                "aggregations", aggsJson);
    }

    @Override
    public AggregationResult parseResponse(JsonObject jsonObject) {
        return AggregationResultParser.parseSubAggregations(jsonObject, subAggregations);
    }
}
