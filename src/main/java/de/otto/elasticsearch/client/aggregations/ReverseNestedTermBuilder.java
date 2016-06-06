package de.otto.elasticsearch.client.aggregations;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import de.otto.elasticsearch.client.response.Aggregation;
import de.otto.elasticsearch.client.response.Bucket;
import de.otto.elasticsearch.client.response.TermsAggregation;

import java.util.ArrayList;

import static de.otto.elasticsearch.client.request.GsonHelper.object;


public class ReverseNestedTermBuilder implements AggregationBuilder {
    private TermsBuilder termsBuilder;
    private String fieldName;

    public ReverseNestedTermBuilder() {
    }

    public ReverseNestedTermBuilder withTermsBuilder(TermsBuilder termsBuilder) {
        this.termsBuilder = termsBuilder;
        return this;
    }

    public ReverseNestedTermBuilder withReverseNestedFieldName(String fieldName) {
        this.fieldName = fieldName;
        return this;
    }

    @Override
    public JsonObject build() {
        JsonObject nestedObject = new JsonObject();
        if (termsBuilder != null) {
            nestedObject.add("terms", termsBuilder.buildValue());
        } else {
            throw new RuntimeException("TermsBuilder must be specified");
        }
        if (fieldName != null) {
            JsonObject innerObject = new JsonObject();
            innerObject.add(fieldName, object("reverse_nested", object()));
            nestedObject.add("aggs", innerObject);
        } else {
            throw new RuntimeException("ReverseNestedBuilder must be specified");
        }
        return nestedObject;
    }

    @Override
    public Aggregation parseResponse(JsonObject jsonObject) {
        Aggregation aggregation = null;

        JsonElement bucketsElement = jsonObject.get("buckets");
        if (bucketsElement != null) {
            JsonArray bucketsArray = bucketsElement.getAsJsonArray();
            ArrayList<Bucket> bucketList = new ArrayList<>();
            for (JsonElement elem : bucketsArray) {
                JsonObject bucket = elem.getAsJsonObject();
                JsonObject reverseNestedAggregationResult = bucket.getAsJsonObject(fieldName);
                if (reverseNestedAggregationResult == null) {
                    throw new RuntimeException("No reverse nested aggregation result for " + fieldName);
                }
                bucketList.add(new Bucket(bucket.get("key").getAsString(), reverseNestedAggregationResult.get("doc_count").getAsLong()));
            }
            aggregation = new Aggregation(new TermsAggregation(bucketList));
        }
        return aggregation;
    }
}
