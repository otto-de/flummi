package de.otto.elasticsearch.client.bulkactions;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import de.otto.elasticsearch.client.util.StringUtils;

import static de.otto.elasticsearch.client.request.GsonHelper.object;


public class DeleteActionBuilder implements BulkActionBuilder {
    private final String indexName;
    private final String id;
    private final String type;
    private final Gson gson;

    public DeleteActionBuilder(String indexName, String id, String type) {
        this.indexName = indexName;
        this.id = id;
        this.type = type;
        this.gson = new Gson();
    }

    @Override
    public String toBulkRequestAction() {
        if (StringUtils.isEmpty(indexName)) {
            throw new RuntimeException("missing property 'index'");
        }
        if (StringUtils.isEmpty(id)) {
            throw new RuntimeException("missing property 'id'");
        }
        if (StringUtils.isEmpty(type)) {
            throw new RuntimeException("missing property 'type'");
        }
        JsonObject bulkObject = object("_index", indexName);
        bulkObject.add("_id", new JsonPrimitive(id));
        bulkObject.add("_type", new JsonPrimitive(type));
        JsonObject jsonObject = object("delete", bulkObject);
        return gson.toJson(jsonObject);
    }
}
