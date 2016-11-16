package de.otto.flummi.query;

import com.google.gson.JsonPrimitive;
import org.testng.annotations.Test;

import static de.otto.flummi.request.GsonHelper.array;
import static de.otto.flummi.request.GsonHelper.object;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class TermsQueryBuilderTest {

    @Test
    public void shouldCreateTerms() {
        TermsQueryBuilder termsQueryBuilder = new TermsQueryBuilder("someName", asList("someValue", "someOtherValue"));
        assertThat(termsQueryBuilder.build(), is(object("terms", object("someName", array(
                new JsonPrimitive("someValue"),
                new JsonPrimitive("someOtherValue")
        )))));
    }

    @Test
    public void shouldThrowexceptionIfNameIsMissing() {
        try {
            new TermsQueryBuilder(null, asList("someValue")).build();
        } catch (RuntimeException e) {
            assertThat(e.getMessage(), is("missing property 'name'"));
        }
    }

    @Test
    public void shouldThrowexceptionIfNameIsEmpty() {
        try {
            new TermsQueryBuilder("", asList("someValue")).build();
        } catch (RuntimeException e) {
            assertThat(e.getMessage(), is("missing property 'name'"));
        }
    }

    @Test
    public void shouldThrowexceptionIfValueIsNull() {
        try {
            new TermsQueryBuilder("someName", null).build();
        } catch (RuntimeException e) {
            assertThat(e.getMessage(), is("missing property 'terms'"));
        }
    }

    @Test
    public void shouldThrowexceptionIfValueIsEmpty() {
        try {
            new TermsQueryBuilder("someName", emptyList()).build();
        } catch (RuntimeException e) {
            assertThat(e.getMessage(), is("missing property 'terms'"));
        }
    }
}