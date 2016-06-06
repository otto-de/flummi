package de.otto.elasticsearch.client.query;

import com.google.gson.JsonObject;
import de.otto.elasticsearch.client.GsonCollectors;

import static java.util.Arrays.stream;

public class AndQueryBuilder implements QueryBuilder {

    private final QueryBuilder[] filters;

    public AndQueryBuilder(QueryBuilder... filters) {
        this.filters = filters;
    }

    @Override
    public JsonObject build() {
        if (filters == null || filters.length == 0) {
            throw new RuntimeException("missing property 'filters'");
        }
        JsonObject jsonObject = new JsonObject();
        jsonObject.add("and", stream(filters)
                .map(filter -> filter.build())
                .collect(GsonCollectors.toJsonArray()));
        return jsonObject;
    }
}
