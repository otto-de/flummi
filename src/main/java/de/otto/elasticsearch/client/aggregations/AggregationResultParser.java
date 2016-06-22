package de.otto.elasticsearch.client.aggregations;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import de.otto.elasticsearch.client.response.AggregationResult;
import de.otto.elasticsearch.client.response.Bucket;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AggregationResultParser {
    public static AggregationResult parseBuckets(JsonObject jsonObject) {
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

    public static AggregationResult parseSubAggregations(JsonObject jsonObject, List<AggregationBuilder<?>> subAggregations) {
        Map<String, AggregationResult> aggregations = new HashMap<>();

        if (subAggregations != null) {
            subAggregations.stream().forEach(t ->
                    aggregations.put(t.getName(), t.parseResponse(jsonObject.get(t.getName()).getAsJsonObject())));
        }

        return new AggregationResult(aggregations);
    }
}
