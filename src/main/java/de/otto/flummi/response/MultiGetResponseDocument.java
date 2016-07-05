package de.otto.flummi.response;

import com.google.gson.JsonObject;

public class MultiGetResponseDocument {

    private final String id;
    private final boolean found;
    private final JsonObject source;

    public MultiGetResponseDocument(String id, boolean found, JsonObject source) {
        this.id = id;
        this.found = found;
        this.source = source;
    }

    public String getId() {
        return id;
    }

    public boolean isFound() {
        return found;
    }

    public JsonObject getSource() {
        return source;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MultiGetResponseDocument that = (MultiGetResponseDocument) o;

        if (found != that.found) return false;
        if (id != null ? !id.equals(that.id) : that.id != null) return false;
        return source != null ? source.equals(that.source) : that.source == null;

    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (found ? 1 : 0);
        result = 31 * result + (source != null ? source.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "MultiGetResponseDocument{" +
                "id='" + id + '\'' +
                ", found=" + found +
                ", source=" + source +
                '}';
    }
}
