package de.otto.flummi.aggregations;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import de.otto.flummi.SortOrder;
import de.otto.flummi.response.AggregationResult;
import de.otto.flummi.response.Bucket;
import de.otto.flummi.response.BucketAggregationResult;
import de.otto.flummi.util.Pair;

import java.util.ArrayList;
import java.util.stream.Collector;

public class TermsBuilder extends SubAggregationBuilder<TermsBuilder> {
    private String fieldName;
    private Integer size;
    private ArrayList<Pair<String, SortOrder>> orders;

    public TermsBuilder(String name) {
        super(name);
    }

    public TermsBuilder field(String fieldName) {
        this.fieldName = fieldName;
        return this;
    }

    public TermsBuilder size(int size) {
        this.size = size;
        return this;
    }

    public TermsBuilder order(String fieldName, SortOrder order) {
        if (this.orders == null) {
            orders = new ArrayList<>();
        }
        orders.add(new Pair<>(fieldName, order));
        return this;
    }

    @Override
    public JsonObject build() {
        if (fieldName==null || fieldName.isEmpty()) {
            throw new RuntimeException("missing property 'field'");
        }
        JsonObject jsonObject = new JsonObject();
        JsonObject fields = new JsonObject();
        jsonObject.add("terms", fields);
        fields.add("field", new JsonPrimitive(fieldName));
        if (size != null) {
            fields.add("size", new JsonPrimitive(size));
        }
        if (orders != null) {
            JsonObject orderObject = new JsonObject();
            orders.forEach(e -> orderObject.add(e.getKey(), new JsonPrimitive(e.getValue().toString())));
            fields.add("order", orderObject);
        }

        if (subAggregations != null) {
            JsonObject subAggregationsObject = subAggregations
                    .stream()
                    .collect(toJsonObject());

            jsonObject.add("aggregations", subAggregationsObject);

        }
        return jsonObject;
    }

    private static Collector<AggregationBuilder, JsonObject, JsonObject> toJsonObject() {
        return Collector.of(JsonObject::new,
                (json, a) -> json.add(a.getName(), a.build()),
                (left, right) -> left);
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

                AggregationResult subAggregationResult = AggregationResultParser.parseSubAggregations(elemObject, subAggregations);

                Bucket bucket = new Bucket(elemObject.get("key").getAsString(), elemObject.get("doc_count").getAsLong(), subAggregationResult.getNestedAggregations());

                bucketList.add(bucket);

            }
            aggregation = new BucketAggregationResult(bucketList);
        }
        return aggregation;
    }

}