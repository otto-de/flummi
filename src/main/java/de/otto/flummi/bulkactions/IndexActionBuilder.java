package de.otto.flummi.bulkactions;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import static de.otto.flummi.request.GsonHelper.object;

public class IndexActionBuilder implements BulkActionBuilder {

    private final String index;
    private final Gson gson;
    private String id;
    private JsonObject source;
    private IndexOpType opType;
    private String parent;

    public IndexActionBuilder(String index) {
        this.index = index;
        this.gson = new Gson();
    }

    public IndexActionBuilder setOpType(IndexOpType opType) {
        this.opType = opType;
        return this;
    }

    public IndexActionBuilder setId(String id) {
        this.id = id;
        return this;
    }

    public IndexActionBuilder setSource(JsonObject source) {
        this.source = source;
        return this;
    }

    public IndexActionBuilder setParent(String parent) {
        this.parent = parent;
        return this;
    }

    @Override
    public String toBulkRequestAction() {
        if (index == null || index.isEmpty()) {
            throw new RuntimeException("missing property 'index'");
        }
        if (opType == null) {
            throw new RuntimeException("missing property 'opType'");
        }
        JsonObject bulkObject = object("_index", index);
        if (id != null && !id.isEmpty()) {
            bulkObject.add("_id", new JsonPrimitive(id));
        }
        if (parent != null && !parent.isEmpty()) {
            bulkObject.add("parent", new JsonPrimitive(parent));
        }
        JsonObject jsonObject = object(opType.opCode(), bulkObject);
        if (IndexOpType.UPDATE.equals(opType)) {
            JsonObject docObject = new JsonObject();
            docObject.add("doc", source);
            return gson.toJson(jsonObject) + "\n" + gson.toJson(docObject);
        }
        return gson.toJson(jsonObject) + "\n" + gson.toJson(source);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IndexActionBuilder that = (IndexActionBuilder) o;

        if (index != null ? !index.equals(that.index) : that.index != null) return false;
        if (gson != null ? !gson.equals(that.gson) : that.gson != null) return false;
        if (id != null ? !id.equals(that.id) : that.id != null) return false;
        if (source != null ? !source.equals(that.source) : that.source != null) return false;
        return opType == that.opType;

    }

    @Override
    public int hashCode() {
        int result = index != null ? index.hashCode() : 0;
        result = 31 * result + (gson != null ? gson.hashCode() : 0);
        result = 31 * result + (id != null ? id.hashCode() : 0);
        result = 31 * result + (source != null ? source.hashCode() : 0);
        result = 31 * result + (opType != null ? opType.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "IndexActionBuilder{" +
                "index='" + index + '\'' +
                ", gson=" + gson +
                ", id='" + id + '\'' +
                ", source=" + source +
                ", opType=" + opType +
                '}';
    }
}
