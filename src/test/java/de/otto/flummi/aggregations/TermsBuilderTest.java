package de.otto.flummi.aggregations;

import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import de.otto.flummi.SortOrder;
import org.testng.annotations.Test;

import static de.otto.flummi.request.GsonHelper.array;
import static de.otto.flummi.request.GsonHelper.object;
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
}