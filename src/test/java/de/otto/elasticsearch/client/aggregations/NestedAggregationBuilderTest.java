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
        NestedAggregationBuilder nestedAggregationBuilder = new NestedAggregationBuilder("nestedAggBuilder")
                .path("categories")
                .subAggregation(new TermsBuilder("assortment_buckets")
                        .field("assortment")
                );

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

    @Test(enabled = false)
    public void shouldBuildNestedRangeAggregation() throws Exception {
        //given
        NestedAggregationBuilder nestedAggregationBuilder = new NestedAggregationBuilder("nested_fieldname")
                .path("variations")
                .subAggregation(new RangeBuilder("preis_ranges")
                        .field("variations.retailPrice")
                        .addUnboundedTo("0bis10EUR", 1000)
                        .addUnboundedTo("0bis20EUR", 2000)
                );

        //then
        String expected = "{\n" +
                "      \"nested\": {\n" +
                "        \"path\": \"variations\"\n" +
                "      },\n" +
                "      \"aggregations\": {\n" +
                "        \"preis_ranges\": {\n" +
                "          \"range\": {\n" +
                "            \"field\": \"variations.retailPrice\",\n" +
                "            \"ranges\":[" +
                "               {\"key\": \"0bis10EUR\", \"to\":1000.0}," +
                "               {\"key\": \"0bis20EUR\", \"to\":2000.0}" +
                "              ]" +
                "           }" +
                "         }" +
                "      }" +
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
            assertThat(e.getMessage(), is("property 'nestedAggregations' is missing"));
        }
        //then
    }

}