package de.otto.elasticsearch.client.response;

import com.google.gson.JsonObject;

public class GetResponse {
    private boolean exists;
    private JsonObject source;

    public GetResponse(boolean exists, JsonObject source) {
        this.exists = exists;
        this.source = source;
    }

    public boolean isExists() {
        return exists;
    }

    public JsonObject getSource() {
        return source;
    }
}
