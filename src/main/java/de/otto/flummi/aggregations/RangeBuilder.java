package de.otto.flummi.aggregations;

import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import de.otto.flummi.response.AggregationResult;

import java.util.ArrayList;
import java.util.List;

import static de.otto.flummi.GsonCollectors.toJsonArray;
import static de.otto.flummi.aggregations.AggregationResultParser.parseBuckets;
import static de.otto.flummi.request.GsonHelper.object;

public class RangeBuilder extends SubAggregationBuilder<RangeBuilder> {

    private String fieldName;
    private final List<Range> ranges = new ArrayList<>();

    public RangeBuilder(String name) {
        super(name);
    }

    @Override
    public JsonObject build() {
        JsonObject rangeAggregatorObject = object(
                "field", new JsonPrimitive(fieldName),
                "ranges", ranges.stream().map(this::rangeToJson).collect(toJsonArray()));
        return object("range", rangeAggregatorObject);
    }

    private JsonObject rangeToJson(Range r) {
        JsonObject result = object("key", new JsonPrimitive(r.getKey()));
        if(r.getFrom()!=null) {
            result.add("from", new JsonPrimitive(r.getFrom()));
        }
        if(r.getTo()!=null) {
            result.add("to", new JsonPrimitive(r.getTo()));
        }
        return result;
    }

    public RangeBuilder field(String fieldName) {
        this.fieldName = fieldName;
        return this;
    }

    public RangeBuilder addRange(String key, double from, double to) {
        ranges.add(new Range(key, from, to));
        return this;
    }

    public RangeBuilder addUnboundedTo(String key, double to) {
        ranges.add(new Range(key, null, to));
        return this;
    }

    public RangeBuilder addUnboundedFrom(String key, double from) {
        ranges.add(new Range(key, from, null));
        return this;
    }

    @Override
    public AggregationResult parseResponse(JsonObject jsonObject) {
        return parseBuckets(jsonObject);
    }
}
