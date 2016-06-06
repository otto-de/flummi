package de.otto.elasticsearch.client.query;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import static de.otto.elasticsearch.client.request.GsonHelper.object;


public class FunctionScoreQueryBuilder implements QueryBuilder {

    private QueryBuilder innerQuery;
    private String scoreMode;
    private FieldValueFactorBuilder scoreFunction;

    public FunctionScoreQueryBuilder(QueryBuilder innerQuery) {
        this.innerQuery = innerQuery;
    }

    public FunctionScoreQueryBuilder add(FieldValueFactorBuilder scoreFunction) {
        this.scoreFunction = scoreFunction;
        return this;
    }

    public FunctionScoreQueryBuilder setScoreMode(String scoreMode) {
        this.scoreMode = scoreMode;
        return this;
    }

    @Override
    public JsonObject build() {
        JsonObject functionScore = new JsonObject();
        functionScore.add("query", innerQuery.build());
        if(scoreMode!=null) {
            functionScore.add("score_mode", new JsonPrimitive(scoreMode));
        }
        if(scoreFunction!=null) {
            JsonArray array = new JsonArray();
            array.add(scoreFunction.build());
            functionScore.add("functions", array);
        }
        JsonObject result = object("function_score", functionScore);
        return result;
    }
}
