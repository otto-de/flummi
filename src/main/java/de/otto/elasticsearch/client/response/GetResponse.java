package de.otto.elasticsearch.client.response;

import com.google.gson.JsonObject;

public class GetResponse {
    private final boolean exists;
    private final JsonObject source;
    private final String id;

    public GetResponse(boolean exists, JsonObject source, String id) {
        this.exists = exists;
        this.source = source;
        this.id = id;
    }

    public boolean isExists() {
        return exists;
    }

    public JsonObject getSource() {
        return source;
    }

    public String getId() {
        return id;
    }
}
