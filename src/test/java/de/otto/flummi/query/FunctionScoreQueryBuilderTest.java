package de.otto.flummi.query;

import com.google.gson.JsonPrimitive;
import org.testng.annotations.Test;

import static de.otto.flummi.request.GsonHelper.array;
import static de.otto.flummi.request.GsonHelper.object;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class FunctionScoreQueryBuilderTest {

    @Test
    public void shouldCreateFunctionScoreQuery() {
        FunctionScoreQueryBuilder functionScoreQueryBuilder = QueryBuilders.functionScoreQuery(QueryBuilders.matchAll())
                .add(new FieldValueFactorBuilder("pred_turnover").setModifier("ln").setFactor(2));
        assertThat(functionScoreQueryBuilder.build(), is(
                object("function_score",
                        object("query",
                                object("match_all", object()),
                                "functions",
                                array(
                                        object("field_value_factor",
                                                object(
                                                        "modifier", new JsonPrimitive("ln"),
                                                        "factor", new JsonPrimitive(2),
                                                        "field", new JsonPrimitive("pred_turnover"))
                                        )
                                )
                        )
                )
                )
        );
    }



}