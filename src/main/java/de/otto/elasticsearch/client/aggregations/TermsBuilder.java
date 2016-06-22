package de.otto.elasticsearch.client.aggregations;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import de.otto.elasticsearch.client.SortOrder;
import de.otto.elasticsearch.client.response.AggregationResult;
import de.otto.elasticsearch.client.util.StringUtils;
import javafx.util.Pair;

import java.util.ArrayList;

import static de.otto.elasticsearch.client.aggregations.AggregationResultParser.parseBuckets;

public class TermsBuilder extends AggregationBuilder<TermsBuilder> {
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
        if (StringUtils.isEmpty(fieldName)) {
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
        return jsonObject;
    }

    @Override
    public AggregationResult parseResponse(JsonObject jsonObject) {
        return parseBuckets(jsonObject);
    }

    public JsonElement buildValue() {
        if (StringUtils.isEmpty(fieldName)) {
            throw new RuntimeException("missing property 'field'");
        }
        JsonObject fields = new JsonObject();
        fields.add("field", new JsonPrimitive(fieldName));
        if (size != null) {
            fields.add("size", new JsonPrimitive(size));
        }
        if (orders != null) {
            JsonObject orderObject = new JsonObject();
            orders.forEach(e -> orderObject.add(e.getKey(), new JsonPrimitive(e.getValue().toString())));
            fields.add("order", orderObject);
        }
        return fields;
    }
}
