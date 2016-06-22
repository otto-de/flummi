package de.otto.elasticsearch.client.aggregations;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import de.otto.elasticsearch.client.response.AggregationResult;
import de.otto.elasticsearch.client.response.Bucket;

import java.util.ArrayList;
import java.util.List;

import static de.otto.elasticsearch.client.GsonCollectors.toJsonArray;
import static de.otto.elasticsearch.client.request.GsonHelper.object;

public class RangeBuilder extends AggregationBuilder<RangeBuilder> {

    private String fieldName;
    private final List<Range> ranges = new ArrayList();

    protected RangeBuilder(String name) {
        super(name);
    }

    @Override
    public JsonObject build() {
        JsonObject rangeAggregatorObject = object(
                "field", new JsonPrimitive(fieldName),
                "ranges", ranges.stream().map(r -> rangeToJson(r)).collect(toJsonArray()));
        return object(getName(), object("range", rangeAggregatorObject));
    }

    private JsonObject rangeToJson(Range r) {
        JsonObject result = object("key", new JsonPrimitive(r.getKey()));
        if(r.getFrom()!=null) {
            result.add("from", new JsonPrimitive(r.getFrom()));
        }
        if(r.getTo()!=null) {
            result.add("from", new JsonPrimitive(r.getTo()));
        }
        return result;
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
        AggregationResult aggregation = null;

        JsonElement bucketsElement = jsonObject.get("buckets");
        if (bucketsElement != null) {
            JsonArray bucketsArray = bucketsElement.getAsJsonArray();
            ArrayList<Bucket> bucketList = new ArrayList<>();
            for (JsonElement elem : bucketsArray) {
                JsonObject elemObject = elem.getAsJsonObject();
                bucketList.add(new Bucket(elemObject.get("key").getAsString(), elemObject.get("doc_count").getAsLong()));
            }
            aggregation = new AggregationResult(bucketList);
        }
        return aggregation;
    }
}
