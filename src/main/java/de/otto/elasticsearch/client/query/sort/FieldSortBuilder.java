package de.otto.elasticsearch.client.query.sort;

import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import de.otto.elasticsearch.client.SortOrder;
import de.otto.elasticsearch.client.query.QueryBuilder;

import static de.otto.elasticsearch.client.request.GsonHelper.object;

public class FieldSortBuilder implements SortBuilder {
    private final String fieldName;

    private SortOrder order;

    private SortMode sortMode;

    private QueryBuilder nestedFilter;

    private String nestedPath;

    public FieldSortBuilder(String fieldName) {
        this.fieldName = fieldName;
    }

    public FieldSortBuilder setOrder(SortOrder order) {
        this.order = order;
        return this;
    }

    public FieldSortBuilder setSortMode(SortMode sortMode) {
        this.sortMode = sortMode;
        return this;
    }

    public FieldSortBuilder setNestedFilter(QueryBuilder nestedFilter) {
        this.nestedFilter = nestedFilter;
        return this;
    }

    public FieldSortBuilder setNestedPath(String nestedPath) {
        this.nestedPath = nestedPath;
        return this;
    }

    @Override
    public JsonObject build() {
        JsonObject sortObject = object("order", order.toString(), "mode", sortMode.key());
        if(nestedFilter!=null) {
            sortObject.add("nested_filter", nestedFilter.build());
        }
        if(nestedPath!=null) {
            sortObject.add("nested_path", new JsonPrimitive(nestedPath));
        }
        return object(fieldName, sortObject);
    }
}
