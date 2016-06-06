package de.otto.elasticsearch.client.query;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

public class BoolQueryBuilder implements QueryBuilder {
    private JsonArray mustFilter = new JsonArray();
    private JsonArray mustNotFilter = new JsonArray();

    @Override
    public JsonObject build() {
        if(mustFilter.size() == 0 && mustNotFilter.size() == 0) {
            throw new RuntimeException("mustFilter and mustNotFilter are empty");
        }
        JsonObject jsonObject = new JsonObject();
        JsonObject boolObject = new JsonObject();
        jsonObject.add("bool", boolObject);
        if (mustFilter.size() > 0) {
            if (mustFilter.size() == 1) {
                boolObject.add("must", mustFilter.get(0));
            } else {
                boolObject.add("must", mustFilter);
            }
        }

        if (mustNotFilter.size() > 0) {
            if (mustNotFilter.size() == 1) {
                boolObject.add("must_not", mustNotFilter.get(0));
            } else {
                boolObject.add("must_not", mustNotFilter);
            }
        }

        return jsonObject;
    }

    public boolean isEmpty() {
        return mustFilter.size() == 0 && mustNotFilter.size() == 0;
    }

    public BoolQueryBuilder must(JsonObject filter) {
        this.mustFilter.add(filter);
        return this;
    }

    public BoolQueryBuilder mustNot(JsonObject filter) {
        this.mustNotFilter.add(filter);
        return this;
    }

    public BoolQueryBuilder must(QueryBuilder queryBuilder) {
        must(queryBuilder.build());
        return this;
    }
    public void mustNot(QueryBuilder queryBuilder) {
        mustNot(queryBuilder.build());
    }
}
