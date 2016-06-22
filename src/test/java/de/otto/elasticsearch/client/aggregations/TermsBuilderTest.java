package de.otto.elasticsearch.client.aggregations;

import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import de.otto.elasticsearch.client.SortOrder;
import de.otto.elasticsearch.client.response.AggregationResult;
import org.testng.annotations.Test;

import static de.otto.elasticsearch.client.request.GsonHelper.array;
import static de.otto.elasticsearch.client.request.GsonHelper.object;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;

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

    @Test
    public void shouldParseResponse() {
        // given
        TermsBuilder termsBuilder = new TermsBuilder("bla").field("someField");
        JsonObject response = object("buckets", array(object("key", "someKey", "doc_count", "1")));

        // when
        AggregationResult aggregation = termsBuilder.parseResponse(response);

        // then
        assertThat(aggregation.getNestedAggregations(), nullValue());
        assertThat(aggregation.getBuckets(), hasSize(1));
        assertThat(aggregation.getBuckets().get(0).getKey(), is("someKey"));
        assertThat(aggregation.getBuckets().get(0).getDocCount(), is(1L));
    }

    @Test
    public void shouldParseResponseWithMultipleBuckets() {
        // given
        TermsBuilder termsBuilder = new TermsBuilder("bla").field("someField");
        JsonObject response = object("buckets", array(object("key", "someKey", "doc_count", "1"), object("key", "someKey2", "doc_count", "3")));

        // when
        AggregationResult aggregation = termsBuilder.parseResponse(response);

        // then
        assertThat(aggregation.getNestedAggregations(), nullValue());
        assertThat(aggregation.getBuckets(), hasSize(2));
        assertThat(aggregation.getBuckets().get(0).getKey(), is("someKey"));
        assertThat(aggregation.getBuckets().get(0).getDocCount(), is(1L));
        assertThat(aggregation.getBuckets().get(1).getKey(), is("someKey2"));
        assertThat(aggregation.getBuckets().get(1).getDocCount(), is(3L));
    }

    @Test
    public void shouldParseResponseWithEmptyBuckets() {
        // given
        TermsBuilder termsBuilder = new TermsBuilder("bla").field("someField");
        JsonObject response = object("buckets", array());

        // when
        AggregationResult aggregation = termsBuilder.parseResponse(response);

        // then
        assertThat(aggregation.getNestedAggregations(), nullValue());
        assertThat(aggregation.getBuckets(), hasSize(0));
    }


}