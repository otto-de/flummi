package de.otto.elasticsearch.client.aggregations;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import de.otto.elasticsearch.client.response.Aggregation;
import de.otto.elasticsearch.client.response.Bucket;
import org.testng.annotations.Test;

import java.util.List;

import static de.otto.elasticsearch.client.request.GsonHelper.array;
import static de.otto.elasticsearch.client.request.GsonHelper.object;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;

public class NestedAggregationBuilderTest {

    @Test
    public void shouldAddPathToQuery() throws Exception {
        //given/when
        NestedAggregationBuilder nestedAggregationBuilder = new NestedAggregationBuilder("nestedAggBuilder").path("categories").subAggregation(
                new TermsBuilder("assortment_buckets").field("assortment"));

        //then
        String expected = "{\n" +
                "      \"nested\": {\n" +
                "        \"path\": \"categories\"\n" +
                "      },\n" +
                "      \"aggregations\": {\n" +
                "        \"assortment_buckets\": {\n" +
                "          \"terms\": {\n" +
                "            \"field\": \"assortment\"\n" +
                "          }\n" +
                "        }\n" +
                "      }\n" +
                "    }";
        JsonObject expectedJson = new Gson().fromJson(expected, JsonObject.class);

        assertThat(nestedAggregationBuilder.build(), is(expectedJson));
    }

    @Test
    public void shouldBuildReverseNestedAggregationForSaleProperty() throws Exception {
        //given/when
        ReverseNestedTermBuilder nestedAggregationBuilder = new ReverseNestedTermBuilder("is_sale")
                .withTermsBuilder(new TermsBuilder("termsBuilder").field("variations.sale"))
                .withReverseNestedFieldName("is_sale_per_product");

        NestedAggregationBuilder reverseNestedSaleAggregation = new NestedAggregationBuilder("nestedAggBuilder")
                .path("variations")
                .subAggregation(nestedAggregationBuilder);

        //then
        String expected = "{\n" +
                "      \"nested\": {\n" +
                "        \"path\": \"variations\"\n" +
                "      },\n" +
                "      \"aggregations\": {\n" +
                "        \"is_sale\": {\n" +
                "          \"terms\": {\n" +
                "            \"field\": \"variations.sale\"\n" +
                "          },\n" +
                "          \"aggs\": {\n" +
                "            \"is_sale_per_product\": {\n" +
                "              \"reverse_nested\": {}\n" +
                "            }\n" +
                "          }\n" +
                "        }\n" +
                "      }\n" +
                "    }";
        JsonObject expectedJson = new Gson().fromJson(expected, JsonObject.class);

        assertThat(reverseNestedSaleAggregation.build(), is(expectedJson));
    }

    @Test
    public void shouldThrowExceptionIfPathIsMissing() throws Exception {
        //given/when
        try {
            new NestedAggregationBuilder("bla").subAggregation(new TermsBuilder("assortment_buckets").field("assortment")).build();
        } catch (RuntimeException e) {
            assertThat(e.getMessage(), is("missing property 'path'"));
        }
        //then
    }

    @Test
    public void shouldThrowExceptionIfPathIsEmpty() throws Exception {
        //given/when
        try {
            new NestedAggregationBuilder("bla").path("").subAggregation(new TermsBuilder("assortment_buckets").field("assortment")).build();
        } catch (RuntimeException e) {
            assertThat(e.getMessage(), is("missing property 'path'"));
        }
        //then
    }

    @Test
    public void shouldThrowExceptionIfSubAggregationIsMissing() throws Exception {
        //given/when
        try {
            new NestedAggregationBuilder("bla").path("categories").build();
        } catch (RuntimeException e) {
            assertThat(e.getMessage(), is("property 'termsAggregation' is missing"));
        }
        //then
    }

    @Test
    public void shouldParseResponseWithNestedTermsQuery() {
        // given
        NestedAggregationBuilder nestedAggregationBuilder = new NestedAggregationBuilder("bla").path("somePath").subAggregation(new TermsBuilder("someName").field("someField"));
        JsonObject response = object("someName", object("buckets", array(object("key", "someKey", "doc_count", "1"))));

        // when
        Aggregation aggregation = nestedAggregationBuilder.parseResponse(response);

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
        NestedAggregationBuilder nestedAggregationBuilder = new NestedAggregationBuilder("bla").path("somePath").subAggregation(new ReverseNestedTermBuilder("someName").withTermsBuilder(new TermsBuilder("someName").field("someField")).withReverseNestedFieldName("fieldPerProduct"));
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