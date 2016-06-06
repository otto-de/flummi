package de.otto.elasticsearch.client.response;

import com.google.gson.JsonObject;

public class SearchHit {

    private final String id;
    private final JsonObject source;
    private JsonObject fields;
    private final Float score;

    public SearchHit(final String id, final JsonObject source, final JsonObject fields, final Float score) {
        this.id = id;
        this.source = source;
        this.fields = fields;
        this.score = score;
    }

    public String getId() {
        return id;
    }

    public JsonObject getSource() {
        return source;
    }

    public Float getScore() {
        return score;
    }

    public JsonObject getFields() {
        return fields;
    }
}
