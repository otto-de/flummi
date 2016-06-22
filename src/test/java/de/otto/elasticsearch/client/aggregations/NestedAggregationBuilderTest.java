package de.otto.elasticsearch.client.aggregations;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.testng.annotations.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

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

}