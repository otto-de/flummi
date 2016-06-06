package de.otto.elasticsearch.client.query;

import com.google.gson.JsonPrimitive;
import org.testng.annotations.Test;

import static de.otto.elasticsearch.client.request.GsonHelper.object;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class TermsQueryBuilderTest {

    @Test
    public void shouldCreateTerms() {
        TermsQueryBuilder termsQueryBuilder = new TermsQueryBuilder("someName", new JsonPrimitive("someValue"));
        assertThat(termsQueryBuilder.build(), is(object("terms", object("someName", "someValue"))));
    }

    @Test
    public void shouldThrowexceptionIfNameIsMissing() {
        try {
            new TermsQueryBuilder(null, new JsonPrimitive("someValue")).build();
        } catch (RuntimeException e) {
            assertThat(e.getMessage(), is("missing property 'name'"));
        }
    }

    @Test
    public void shouldThrowexceptionIfNameIsEmpty() {
        try {
            new TermsQueryBuilder("", new JsonPrimitive("someValue")).build();
        } catch (RuntimeException e) {
            assertThat(e.getMessage(), is("missing property 'name'"));
        }
    }

    @Test
    public void shouldThrowexceptionIfValueIsMissing() {
        try {
            new TermsQueryBuilder("someName", null).build();
        } catch (RuntimeException e) {
            assertThat(e.getMessage(), is("missing property 'value'"));
        }
    }
}