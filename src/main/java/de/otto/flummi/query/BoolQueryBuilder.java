package de.otto.flummi.query;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import java.util.ArrayList;

public class BoolQueryBuilder implements QueryBuilder {
    private JsonArray mustFilter = null;
    private JsonArray mustNotFilter = null;
    private JsonArray shouldFilter = null;
    private String minimumShouldMatch = null;

    @Override
    public JsonObject build() {
        if(mustFilter==null && mustNotFilter==null && shouldFilter==null) {
            throw new RuntimeException("mustFilter and mustNotFilter are empty");
        }
        JsonObject jsonObject = new JsonObject();
        JsonObject boolObject = new JsonObject();
        jsonObject.add("bool", boolObject);
        if (mustFilter != null) {
            if (mustFilter.size() == 1) {
                boolObject.add("must", mustFilter.get(0));
            } else {
                boolObject.add("must", mustFilter);
            }
        }

        if (mustNotFilter!=null) {
            if (mustNotFilter.size() == 1) {
                boolObject.add("must_not", mustNotFilter.get(0));
            } else {
                boolObject.add("must_not", mustNotFilter);
            }
        }
        if (shouldFilter!=null) {
            if (shouldFilter.size() == 1) {
                boolObject.add("should", shouldFilter.get(0));
            } else {
                boolObject.add("should", shouldFilter);
            }
        }
        if(minimumShouldMatch!=null) {
            boolObject.add("minimum_should_match", new JsonPrimitive(minimumShouldMatch));
        }
        return jsonObject;
    }

    public boolean isEmpty() {
        return mustFilter.size() == 0 && mustNotFilter.size() == 0;
    }

    public BoolQueryBuilder must(JsonObject filter) {
        if(this.mustFilter == null) {
            this.mustFilter = new JsonArray();
        }
        this.mustFilter.add(filter);
        return this;
    }

    public BoolQueryBuilder mustNot(JsonObject filter) {
        if(this.mustNotFilter == null) {
            this.mustNotFilter = new JsonArray();
        }
        this.mustNotFilter.add(filter);
        return this;
    }

    public BoolQueryBuilder must(QueryBuilder queryBuilder) {
        must(queryBuilder.build());
        return this;
    }
    public BoolQueryBuilder should(QueryBuilder queryBuilder) {
        should(queryBuilder.build());
        return this;
    }

    public BoolQueryBuilder should(JsonObject shouldFilter) {
        if(this.shouldFilter==null) {
            this.shouldFilter = new JsonArray();
        }
        this.shouldFilter.add(shouldFilter);
        return this;
    }

    public BoolQueryBuilder mustNot(QueryBuilder queryBuilder) {
        mustNot(queryBuilder.build());
        return this;
    }

    public BoolQueryBuilder minimumShouldMatch(String s) {
        this.minimumShouldMatch = s;
        return this;
    }
}
