package de.otto.elasticsearch.client.aggregations;

import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import de.otto.elasticsearch.client.response.AggregationResult;
import de.otto.elasticsearch.client.response.Bucket;
import org.testng.annotations.Test;

import java.util.List;

import static de.otto.elasticsearch.client.query.QueryBuilders.matchAll;
import static de.otto.elasticsearch.client.request.GsonHelper.array;
import static de.otto.elasticsearch.client.request.GsonHelper.object;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;

public class FilterAggregationBuilderTest {
    @Test
    public void shouldBuildQuery() throws Exception {
        JsonObject result = new FilterAggregationBuilder("filterBuilder")
                .withFilter(matchAll())
                .subAggregation(
                        new TermsBuilder("termsBuilder")
                                .field("bla")
                                .size(10)
                ).build();

        assertThat(result.toString(), is("{\"filter\":{\"match_all\":{}},\"aggregations\":{\"termsBuilder\":{\"terms\":{\"field\":\"bla\",\"size\":10}}}}"));

    }
}