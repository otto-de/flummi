package de.otto.flummi.aggregations;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import de.otto.flummi.response.AggregationResult;
import de.otto.flummi.response.Bucket;

import java.util.ArrayList;

import static de.otto.flummi.request.GsonHelper.object;


/**
 * Simplified ReverseNestedAggregationBuilder for cases where each outer bucket has only
 * one inner bucket
 */
public class ReverseNestedBuilder extends AggregationBuilder<ReverseNestedBuilder> {
    private final AggregationBuilder<?> innerAggregation;
    private NestedAggregationBuilder nestedAggregation;

    public ReverseNestedBuilder(String name, String nestedPath, AggregationBuilder<?> innerAggregation) {
        super(name);
        this.innerAggregation = innerAggregation;
        this.nestedAggregation = new NestedAggregationBuilder(name).path(nestedPath).subAggregation(innerAggregation);
    }

    @Override
    public ReverseNestedBuilder subAggregation(AggregationBuilder subAggregation) {
        throw new IllegalStateException("ReverseNestedBuilder does not support further nested aggregations");
    }

    @Override
    public JsonObject build() {
        JsonObject result = nestedAggregation.build();
        JsonObject innerAggregationJson = result.getAsJsonObject("aggregations").getAsJsonObject(innerAggregation.getName());
        innerAggregationJson.add("aggs", object(getName(), object("reverse_nested", object())));
        return result;
    }

    @Override
    public AggregationResult parseResponse(JsonObject jsonObject) {
        AggregationResult aggregation = null;
        JsonObject innerAggregation = jsonObject.getAsJsonObject(getName());
        JsonElement bucketsElement = innerAggregation.get("buckets");
        if (bucketsElement != null) {
            JsonArray bucketsArray = bucketsElement.getAsJsonArray();
            ArrayList<Bucket> bucketList = new ArrayList<>();
            for (JsonElement elem : bucketsArray) {
                JsonObject outerBucket = elem.getAsJsonObject();
                JsonObject innerBucket = outerBucket.getAsJsonObject(getName());
                if (innerBucket == null) {
                    throw new RuntimeException("No reverse nested aggregation result for " + getName());
                }
                bucketList.add(new Bucket(outerBucket.get("key").getAsString(), innerBucket.get("doc_count").getAsLong()));
            }
            aggregation = new AggregationResult(bucketList);
        }
        return aggregation;
    }
}
