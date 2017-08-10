package de.otto.flummi.aggregations;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import de.otto.flummi.SortOrder;
import de.otto.flummi.response.AggregationResult;
import de.otto.flummi.response.Bucket;
import org.testng.annotations.Test;

import static de.otto.flummi.request.GsonHelper.object;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;

public class TermsBuilderTest {

    @Test
    public void shouldAddFieldToTermsQuery() throws Exception {
        // given
        // when
        TermsBuilder termsBuilder = new TermsBuilder("bla").field("someField");
        //then
        assertThat(termsBuilder.build(), is(object("terms", object("field", new JsonPrimitive("someField")))));
    }

    @Test
    public void shouldAddSizeToTermsQuery() throws Exception {
        // given
        // when
        TermsBuilder termsBuilder = new TermsBuilder("bla").field("someField").size(5);
        //then
        JsonObject termsObject = new JsonObject();
        termsObject.add("field", new JsonPrimitive("someField"));
        termsObject.add("size", new JsonPrimitive(5));
        assertThat(termsBuilder.build(), is(object("terms", termsObject)));
    }

    @Test
    public void shouldAddMinDocCountToTermsQuery() throws Exception {
        // given
        // when
        TermsBuilder termsBuilder = new TermsBuilder("bla").field("someField").minDocCount(5);
        //then
        JsonObject termsObject = new JsonObject();
        termsObject.add("field", new JsonPrimitive("someField"));
        termsObject.add("min_doc_count", new JsonPrimitive(5));
        assertThat(termsBuilder.build(), is(object("terms", termsObject)));
    }

    @Test
    public void shouldAddOneOrderToTermsQuery() throws Exception {
        // given
        // when
        TermsBuilder termsBuilder = new TermsBuilder("bla").field("someField").order("someOtherField", SortOrder.ASC);
        //then
        JsonObject termsObject = new JsonObject();
        termsObject.add("field", new JsonPrimitive("someField"));
        JsonObject orderObject = new JsonObject();
        orderObject.add("someOtherField", new JsonPrimitive(SortOrder.ASC.toString()));
        termsObject.add("order", orderObject);
        assertThat(termsBuilder.build(), is(object("terms", termsObject)));
    }

    @Test
    public void shouldAddTwoOrderToTermsQuery() throws Exception {
        // given
        // when
        TermsBuilder termsBuilder = new TermsBuilder("bla").field("someField").order("someOtherField", SortOrder.ASC).order("someOtherOtherField", SortOrder.DESC);
        //then
        JsonObject termsObject = new JsonObject();
        termsObject.add("field", new JsonPrimitive("someField"));
        JsonObject orderObject = new JsonObject();
        orderObject.add("someOtherField", new JsonPrimitive(SortOrder.ASC.toString()));
        orderObject.add("someOtherOtherField", new JsonPrimitive(SortOrder.DESC.toString()));
        termsObject.add("order", orderObject);
        assertThat(termsBuilder.build(), is(object("terms", termsObject)));
    }

    @Test
    public void shouldAddSubAggregationToTermsQuery() throws Exception {
        // given
        // when
        AggregationBuilder termsBuilder = new TermsBuilder("bla").field("someField")
                .subAggregation(new TermsBuilder("subBla").field("subSomeField"));
        //then
        JsonObject result = termsBuilder.build();
        assertThat(result.toString(), is("{\"terms\":{\"field\":\"someField\"},\"aggregations\":{\"subBla\":{\"terms\":{\"field\":\"subSomeField\"}}}}"));
    }

    @Test
    public void shouldParseWithSubAggregation() throws Exception {
        // given
        AggregationBuilder termsBuilder = new TermsBuilder("bla").field("someField")
                .subAggregation(new TermsBuilder("subBla").field("subSomeField"));

        String response =
                "{" +
                        "     \"took\": 29," +
                        "     \"aggregations\": {" +
                        "         \"bla\": {" +
                        "             \"buckets\": [" +
                        "                 {" +
                        "                     \"key\": \"BROOKLYN\"," +
                        "                     \"doc_count\": 1273130," +
                        "                     \"subBla\": {" +
                        "                         \"buckets\": [" +
                        "                             {" +
                        "                                 \"key\": \"S\"," +
                        "                                 \"doc_count\": 38119" +
                        "                             }," +
                        "                             {" +
                        "                                 \"key\": \"M\"," +
                        "                                 \"doc_count\": 40161" +
                        "                             }," +
                        "                             {" +
                        "                                 \"key\": \"L\"," +
                        "                                 \"doc_count\": 39236" +
                        "                             }" +
                        "                         ]" +
                        "                     }" +
                        "                 }" +
                        "             ]" +
                        "         }" +
                        "     }" +
                        "}";

        JsonObject responseJson = new Gson().fromJson(response, JsonObject.class);
        AggregationResult aggregationResult = termsBuilder.parseResponse(responseJson.getAsJsonObject("aggregations").getAsJsonObject("bla"));

        assertThat(aggregationResult.getBuckets(), hasSize(1));

        Bucket termsBucket = aggregationResult.getBuckets().get(0);
        assertThat(termsBucket.getKey(), equalTo("BROOKLYN"));
        assertThat(termsBucket.getDocCount(), equalTo(1273130L));

        assertThat(termsBucket.getAggregations().keySet(), hasSize(1));
        assertThat(termsBucket.getAggregations().keySet(), containsInAnyOrder("subBla"));

        assertThat(termsBucket.getAggregations().get("subBla").getBuckets(), containsInAnyOrder(
                new Bucket("S", 38119L),
                new Bucket("M", 40161L),
                new Bucket("L", 39236L)
        ));
    }

    @Test
    public void shouldThrowExceptionIfFieldIsMissing() throws Exception {
        // given
        // when
        try {
            new TermsBuilder("bla").build();
        }catch (RuntimeException e) {
            assertThat(e.getMessage(), is("missing property 'field'"));
        }
        //then
    }

    @Test
    public void shouldThrowExceptionIfFieldIsEmpty() throws Exception {
        // given
        // when
        try {
            new TermsBuilder("bla").field("").build();
        }catch (RuntimeException e) {
            assertThat(e.getMessage(), is("missing property 'field'"));
        }
        //then
    }
}