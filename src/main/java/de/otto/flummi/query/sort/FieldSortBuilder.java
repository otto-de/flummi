package de.otto.flummi.query.sort;

import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import de.otto.flummi.SortOrder;
import de.otto.flummi.query.QueryBuilder;

import static de.otto.flummi.request.GsonHelper.object;

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
