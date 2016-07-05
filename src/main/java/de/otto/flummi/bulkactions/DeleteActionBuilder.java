package de.otto.flummi.bulkactions;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import static de.otto.flummi.request.GsonHelper.object;


public class DeleteActionBuilder implements BulkActionBuilder {
    private final String indexName;
    private final String id;
    private final String type;
    private String routing;
    private final Gson gson;

    public DeleteActionBuilder(String indexName, String id, String type) {
        this.indexName = indexName;
        this.id = id;
        this.type = type;
        this.gson = new Gson();
    }

    @Override
    public String toBulkRequestAction() {
        if (indexName==null || indexName.isEmpty()) {
            throw new RuntimeException("missing property 'index'");
        }
        if (id==null || id.isEmpty()) {
            throw new RuntimeException("missing property 'id'");
        }
        if (type==null || type.isEmpty()) {
            throw new RuntimeException("missing property 'type'");
        }
        JsonObject bulkObject = object("_index", indexName);
        bulkObject.add("_id", new JsonPrimitive(id));
        bulkObject.add("_type", new JsonPrimitive(type));
        if(routing!=null) {
            bulkObject.add("_routing", new JsonPrimitive(routing));
        }
        JsonObject jsonObject = object("delete", bulkObject);
        return gson.toJson(jsonObject);
    }

    public DeleteActionBuilder setRouting(String routing) {
        this.routing = routing;
        return this;
    }
}
