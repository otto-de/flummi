package de.otto.flummi.aggregations;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import de.otto.flummi.response.AggregationResult;
import de.otto.flummi.response.Bucket;
import org.testng.annotations.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;

public class ReverseNestedBuilderTest {

    @Test
    public void shouldBuildReverseNestedAggregationForSaleProperty() throws Exception {
        //given/when
        ReverseNestedBuilder testee = new ReverseNestedBuilder("is_sale_per_product", "variations", new TermsBuilder("is_sale").field("variations.sale"));

        //then
        String expected = "{\n" +
                "  \"nested\": {\n" +
                "    \"path\": \"variations\"\n" +
                "  },\n" +
                "  \"aggregations\": {\n" +
                "    \"is_sale\": {\n" +
                "      \"terms\": {\n" +
                "        \"field\": \"variations.sale\"\n" +
                "      },\n" +
                "      \"aggs\": {\n" +
                "        \"is_sale_per_product\": {\n" +
                "          \"reverse_nested\": {}\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";
        JsonObject expectedJson = new Gson().fromJson(expected, JsonObject.class);

        assertThat(testee.build(), is(expectedJson));
    }

    @Test
    public void shouldWorkWithTermsAggregation() throws Exception {
        // given
        ReverseNestedBuilder testee = new ReverseNestedBuilder("fashionSizeUS", "variations",
                new TermsBuilder("fashionSizeUS").field("fashionSizeUS")
        );
        String response =
                "{" +
                        "     \"took\": 467," +
                        "     \"aggregations\": {" +
                        "         \"fashionSizeUS\": {" +
                        "             \"doc_count\": 1273130," +
                        "             \"fashionSizeUS\": {" +
                        "                 \"buckets\": [" +
                        "                    {" +
                        "                       \"key\": \"S\"," +
                        "                       \"doc_count\": 38119," +
                        "                       \"fashionSizeUS\": {" +
                        "                           \"doc_count\": 22302" +
                        "                       }" +
                        "                    }," +
                        "                    {" +
                        "                       \"key\": \"M\"," +
                        "                       \"doc_count\": 40161," +
                        "                       \"fashionSizeUS\": {" +
                        "                           \"doc_count\": 23531" +
                        "                       }" +
                        "                    }," +
                        "                    {" +
                        "                       \"key\": \"L\"," +
                        "                       \"doc_count\": 39236," +
                        "                       \"fashionSizeUS\": {" +
                        "                           \"doc_count\": 23123" +
                        "                       }" +
                        "                    }" +
                        "                 ]" +
                        "             }" +
                        "         }" +
                        "     }" +
                        "}";

        JsonObject responseJson = new Gson().fromJson(response, JsonObject.class);
        AggregationResult aggregationResult = testee.parseResponse(responseJson.getAsJsonObject("aggregations").getAsJsonObject("fashionSizeUS"));

        assertThat(aggregationResult.getBuckets(), hasSize(3));
        assertThat(aggregationResult.getBuckets(), containsInAnyOrder(
                new Bucket("S", 22302L),
                new Bucket("M", 23531L),
                new Bucket("L", 23123L)
        ));
    }

    @Test
    public void shouldWorkWithRangeAggregation() throws Exception {
        // given
        ReverseNestedBuilder testee = new ReverseNestedBuilder("preis_produkt", "variations",
                new RangeBuilder("preis")
                        .addUnboundedTo("0bis50EUR", 5000)
                        .addUnboundedTo("0bis100EUR", 10000)
                        .addUnboundedTo("0bis200EUR", 20000)
        );
        String response =
                "{" +
                        "     \"took\": 467," +
                        "     \"aggregations\": {" +
                        "         \"preis_produkt\": {" +
                        "             \"doc_count\": 1273130," +
                        "             \"preis_produkt\": {" +
                        "                 \"buckets\": [" +
                        "                    {" +
                        "                       \"key\": \"0bis50EUR\"," +
                        "                       \"to\": 5000," +
                        "                       \"to_as_string\": \"5000.0\"," +
                        "                       \"doc_count\": 680287," +
                        "                       \"preis_produkt\": {" +
                        "                           \"doc_count\": 211747" +
                        "                       }" +
                        "                    }," +
                        "                    {" +
                        "                       \"key\": \"0bis100EUR\"," +
                        "                       \"to\": 10000," +
                        "                       \"to_as_string\": \"10000.0\"," +
                        "                       \"doc_count\": 994540," +
                        "                       \"preis_produkt\": {" +
                        "                           \"doc_count\": 273328" +
                        "                       }" +
                        "                    }," +
                        "                    {" +
                        "                       \"key\": \"0bis200EUR\"," +
                        "                       \"to\": 20000," +
                        "                       \"to_as_string\": \"20000.0\"," +
                        "                       \"doc_count\": 1118436," +
                        "                       \"preis_produkt\": {" +
                        "                           \"doc_count\": 307248" +
                        "                       }" +
                        "                    }" +
                        "                 ]" +
                        "             }" +
                        "         }" +
                        "     }" +
                        "}";

        JsonObject responseJson = new Gson().fromJson(response, JsonObject.class);
        AggregationResult aggregationResult = testee.parseResponse(responseJson.getAsJsonObject("aggregations").getAsJsonObject("preis_produkt"));

        assertThat(aggregationResult.getBuckets(), hasSize(3));
        assertThat(aggregationResult.getBuckets(), containsInAnyOrder(
                new Bucket("0bis50EUR", 211747L),
                new Bucket("0bis100EUR", 273328L),
                new Bucket("0bis200EUR", 307248L)
        ));
    }

}