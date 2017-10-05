package de.otto.flummi.query;

import com.google.gson.JsonObject;
import de.otto.flummi.GsonCollectors;

import java.util.List;

import static java.util.Arrays.asList;

public class AndQueryBuilder implements QueryBuilder {

    private final List<QueryBuilder> queries;

    public AndQueryBuilder(QueryBuilder... queries) {
        this(asList(queries));
    }

    public AndQueryBuilder(List<QueryBuilder> queries) {
        this.queries = queries;
    }

    @Override
    public JsonObject build() {
        if (queries == null || queries.isEmpty()) {
            throw new RuntimeException("missing property 'queries'");
        }
        JsonObject jsonObject = new JsonObject();
        jsonObject.add("and", queries.stream()
                .map(filter -> filter.build())
                .collect(GsonCollectors.toJsonArray()));
        return jsonObject;
    }
}
