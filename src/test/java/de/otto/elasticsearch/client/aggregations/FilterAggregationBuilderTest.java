package de.otto.elasticsearch.client.aggregations;

import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import de.otto.elasticsearch.client.response.Aggregation;
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

    @Test
    public void shouldParseResponseWithNestedTermsQuery() {
        // given
        FilterAggregationBuilder filterAggregationBuilder = new FilterAggregationBuilder("bla")
                .subAggregation(new TermsBuilder("someName").field("someField"));
        JsonObject response = object("someName", object("buckets", array(object("key", "someKey", "doc_count", "1"))));

        // when
        Aggregation aggregation = filterAggregationBuilder.parseResponse(response);

        // then
        assertThat(aggregation.getTermsAggregation(), nullValue());
        List<Bucket> buckets = aggregation.getNestedAggregations().get("someName").getTermsAggregation().getBuckets();
        assertThat(buckets, hasSize(1));
        assertThat(buckets.get(0).getKey(), is("someKey"));
        assertThat(buckets.get(0).getDocCount(), is(1L));
    }

    @Test
    public void shouldParseResponseWithReverseNestedTermsQuery() {
        // given
        FilterAggregationBuilder nestedAggregationBuilder = new FilterAggregationBuilder("bla").subAggregation(
                new ReverseNestedTermBuilder("someName")
                        .withTermsBuilder(new TermsBuilder("someName").field("someField"))
                        .withReverseNestedFieldName("fieldPerProduct"));
        JsonObject response = object("someName", object("buckets", array(object("key", new JsonPrimitive("someKey"), "fieldPerProduct", object("doc_count", new JsonPrimitive(2))))));

        // when
        Aggregation aggregation = nestedAggregationBuilder.parseResponse(response);

        // then
        assertThat(aggregation.getTermsAggregation(), nullValue());
        List<Bucket> buckets = aggregation.getNestedAggregations().get("someName").getTermsAggregation().getBuckets();
        assertThat(buckets, hasSize(1));
        assertThat(buckets.get(0).getKey(), is("someKey"));
        assertThat(buckets.get(0).getDocCount(), is(2L));
    }
}