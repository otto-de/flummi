package de.otto.flummi.aggregations;

import com.google.gson.JsonPrimitive;
import de.otto.flummi.response.SumAggregationResult;
import org.testng.annotations.Test;

import static de.otto.flummi.request.GsonHelper.object;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class SumAggregationBuilderTest {

    @Test
    public void shouldBuildAggregationQueryWithField() throws Exception {
        // given, when
        SumAggregationBuilder aggregationBuilder = new SumAggregationBuilder("mySumAggregation", "f1");

        // then
        assertThat(aggregationBuilder.build(), is(object("sum", object("field", "f1"))));
    }

    @Test
    public void shouldParseAggregationResult() throws Exception {
        // given, when
        SumAggregationBuilder aggregationBuilder = new SumAggregationBuilder("mySumAggregation", "f1");

        // then
        assertThat(aggregationBuilder.parseResponse(object("value", new JsonPrimitive(2.99))), is(new SumAggregationResult(2.99)));
    }
}